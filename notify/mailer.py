"""Email delivery, behind an interface so tests never send mail.

Two failure classes, and telling them apart is the whole job:

* **hard bounce** — the mailbox does not exist. Retrying is pointless *and
  harmful*: repeated sends to dead addresses wreck the sending domain's
  reputation, and SES suspends an account over ~5% bounce rate. At that point
  nothing is delivered to anyone, so one bad address becomes everybody's outage.
* **soft failure** — timeout, throttle, temporary refusal. Retry with backoff.
"""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Protocol

FROM_ADDR = os.environ.get("NOTIFY_FROM", "alerts@observatory.services")
SMTP_HOST = os.environ.get("NOTIFY_SMTP_HOST", "localhost")
SMTP_PORT = int(os.environ.get("NOTIFY_SMTP_PORT", "1025"))
SMTP_USER = os.environ.get("NOTIFY_SMTP_USER", "")
SMTP_PASS = os.environ.get("NOTIFY_SMTP_PASS", "")
SMTP_TLS = os.environ.get("NOTIFY_SMTP_TLS", "false").lower() == "true"


class HardBounce(Exception):
    """The address is not deliverable. Do not retry; mark it undeliverable."""


class SoftFailure(Exception):
    """Transient. Retry with backoff."""


class Mailer(Protocol):
    def send(self, to: str, subject: str, body: str) -> None: ...


class SMTPMailer:
    """Works against SES's SMTP endpoint, or MailHog locally."""

    def send(self, to: str, subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["From"] = FROM_ADDR
        msg["To"] = to
        msg["Subject"] = subject
        # Marks this as an automated alert so mail clients and mailing lists do
        # not generate auto-replies or vacation responses back at us.
        msg["Auto-Submitted"] = "auto-generated"
        msg.set_content(body)

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
                if SMTP_TLS:
                    s.starttls()
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
        except smtplib.SMTPRecipientsRefused as e:
            raise HardBounce(f"recipient refused: {e}") from e
        except smtplib.SMTPResponseException as e:
            # 5xx is permanent, 4xx is temporary. Treating a 5xx as retryable is
            # how a single bad address turns into a reputation problem.
            if 500 <= e.smtp_code < 600:
                raise HardBounce(f"{e.smtp_code} {e.smtp_error}") from e
            raise SoftFailure(f"{e.smtp_code} {e.smtp_error}") from e
        except (smtplib.SMTPException, OSError) as e:
            raise SoftFailure(str(e)) from e


class FakeMailer:
    """Records instead of sending. Can be told to fail, to exercise both paths."""

    def __init__(self, fail_with: Exception | None = None):
        self.sent: list[tuple[str, str, str]] = []
        self.fail_with = fail_with

    def send(self, to: str, subject: str, body: str) -> None:
        if self.fail_with:
            raise self.fail_with
        self.sent.append((to, subject, body))


def render(sites: list[dict]) -> tuple[str, str]:
    """One email covering every affected site for one person.

    Coalescing is why this takes a list. Someone with grants on twenty cameras
    gets one email naming twenty sites, not twenty emails — a better alert, and
    ~4x less work at 200 sites.
    """
    if not sites:
        raise ValueError("render called with no sites")

    soonest = min(s["predicted_for"] for s in sites)
    if len(sites) == 1:
        subject = f"Rain expected at {sites[0]['device_id']} around {soonest:%H:%M} UTC"
    else:
        subject = f"Rain expected at {len(sites)} sites, earliest {soonest:%H:%M} UTC"

    lines = [
        "Precipitation is forecast within the next hour at the following sites.",
        "Close or park the affected instruments.",
        "",
    ]
    for s in sorted(sites, key=lambda x: x["predicted_for"]):
        prob = f"{s['probability']:.0%}" if s.get("probability") is not None else "unknown"
        lines.append(f"  {s['device_id']}: {prob} chance at {s['predicted_for']:%Y-%m-%d %H:%M} UTC")
    lines += [
        "",
        "This is an automated forecast alert from Observatory Services.",
        "Forecasts are probabilistic; treat this as a prompt to check, not a certainty.",
    ]
    return subject, "\n".join(lines)
