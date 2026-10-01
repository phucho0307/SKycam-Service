#!/usr/bin/env python3
"""fake_pi.py — a sky camera that isn't there.

Speaks the real contract in proto/skycam/v1/skycam.proto to the Go ingest
service over gRPC. There is no ZWO camera and no I2C sensor: frames and
readings are synthesised, so the pipeline can be exercised on a laptop.

What is faithful to the real Pi (skycam-pi/send.py):
  - the same two-tier cadence: JPEG preview every cycle, full FITS every 60s
  - the same "settings override env defaults" rule, applied to the preview
  - the same credential shape: one bearer token, never any S3 keys

What is deliberately DIFFERENT from send.py, and is the point of this script:
  1. Two streams, not four POSTs. DeviceSession (bidirectional) carries
     telemetry up and settings/commands down. UploadFrame (client-streaming)
     carries frames. Separate streams so a 25MB upload cannot head-of-line
     block a command queued behind it.
  2. Frames are spooled to disk before upload, so a failed upload can be
     retried instead of being lost with the exception.
  3. Capture and upload run in separate threads, so the camera never waits
     for the network.
  4. Interrupted FITS uploads resume: GetUploadStatus reports how many bytes
     are durably in S3, and the next attempt starts from that offset.

Config (env):
  OBS_GRPC_TARGET     host:port of the ingest service   (default localhost:9090)
  OBS_INGEST_TOKEN    device bearer token               (REQUIRED)
  OBS_DEVICE_ID       default "fake-skycam"
  OBS_INTERVAL_S      seconds between captures          (default 2)
  OBS_FITS_INTERVAL_S seconds between full FITS         (default 60)
  OBS_FITS_MB         synthetic FITS size in MB         (default 24)
  OBS_CHUNK_KB        FITS chunk size                   (default 512)
  OBS_PREVIEW_PX      preview edge in px                (default 1024)
  OBS_SPOOL_DIR       where un-uploaded frames wait     (default ./spool)

  python fake_pi.py                       run until Ctrl-C
  python fake_pi.py --cycles 5            capture 5 frames, drain, exit
  python fake_pi.py --fits-every 1        a FITS on every cycle
  python fake_pi.py --fail-upload-at-mb 10
      drop the connection 10MB into each FITS, once per frame, then let the
      retry resume. This is the resumable-upload demo.
"""
import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import queue
import random
import shutil
import sys
import threading
import time
import uuid

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))
from google.protobuf.timestamp_pb2 import Timestamp  # noqa: E402
from skycam.v1 import skycam_pb2 as pb  # noqa: E402
from skycam.v1 import skycam_pb2_grpc as rpc  # noqa: E402

TARGET = os.environ.get("OBS_GRPC_TARGET", "localhost:9090")
# TLS off for a local server (which serves h2c), on for anything through
# Cloudflare/Traefik. Set OBS_GRPC_TLS=1 with a hostname and no port.
USE_TLS = os.environ.get("OBS_GRPC_TLS", "").lower() in ("1", "true", "yes")
# PEM to verify the server with, for testing against a self-signed Traefik.
# Unset means the system trust store, which is what production uses.
CA_FILE = os.environ.get("OBS_GRPC_CA", "")
TOKEN = os.environ.get("OBS_INGEST_TOKEN", "")
DEVICE_ID = os.environ.get("OBS_DEVICE_ID", "fake-skycam")
INTERVAL_S = float(os.environ.get("OBS_INTERVAL_S", "2"))
FITS_INTERVAL_S = float(os.environ.get("OBS_FITS_INTERVAL_S", "60"))
FITS_MB = int(os.environ.get("OBS_FITS_MB", "24"))
CHUNK_BYTES = int(os.environ.get("OBS_CHUNK_KB", "512")) * 1024
PREVIEW_PX = int(os.environ.get("OBS_PREVIEW_PX", "1024"))
SPOOL_DIR = os.environ.get("OBS_SPOOL_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spool"))
CLIENT_VERSION = "fake-pi/0.1"

# Env defaults, overridden by whatever the server pushes down the session.
EXPOSURE_MS = float(os.environ.get("OBS_EXPOSURE_MS", "1000"))
GAIN = int(os.environ.get("OBS_GAIN", "200"))

HEARTBEAT_S = 25.0  # server's KeepaliveEnforcementPolicy.MinTime is 20s
UPLOAD_ATTEMPTS = 5  # then the frame moves to spool/failed — a device-side DLQ

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(threadName)-9s %(message)s")
log = logging.getLogger("fake-pi")


def now_ts() -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt.datetime.now(dt.timezone.utc))
    return ts


# ---------------------------------------------------------------------------
# Synthetic FITS. Hand-built rather than via astropy: the header is the part
# that has to be right, and writing it by hand keeps the dependency list to
# grpcio + pillow. Pixels are noise — nothing downstream reads FITS pixels yet
# (cloud detection works on the JPEG preview).
# ---------------------------------------------------------------------------
BLOCK = 2880  # FITS is written in 2880-byte blocks, 36 cards of 80 bytes


def _card(key: str, value, comment: str = "") -> bytes:
    if isinstance(value, bool):
        field = ("T" if value else "F").rjust(20)
    elif isinstance(value, int):
        field = str(value).rjust(20)
    elif isinstance(value, float):
        field = f"{value:.8E}".rjust(20)
    else:
        field = f"'{value}'".ljust(20)
    card = f"{key:<8}= {field}"
    if comment:
        card = f"{card} / {comment}"
    return card[:80].ljust(80).encode("ascii")


def make_fits(width: int, height: int, captured_at: str, exposure_ms: float, gain: int,
              temperature_c, probe_temp_c) -> bytes:
    cards = [
        _card("SIMPLE", True, "conforms to FITS standard"),
        _card("BITPIX", 16, "16-bit signed integers"),
        _card("NAXIS", 2),
        _card("NAXIS1", width),
        _card("NAXIS2", height),
        # The sensor is unsigned 16-bit; FITS BITPIX 16 is signed, so the
        # standard trick is to store value-32768 and set BZERO to shift back.
        _card("BZERO", 32768, "offset for unsigned 16-bit data"),
        _card("BSCALE", 1),
        _card("DATE-OBS", captured_at, "UTC start of exposure"),
        _card("EXPTIME", exposure_ms / 1000.0, "exposure [s]"),
        _card("GAIN", gain, "camera gain"),
        _card("DEVICE", DEVICE_ID, "capture device id"),
        _card("ORIGIN", "fake-pi", "SYNTHETIC DATA - not a real observation"),
    ]
    if temperature_c is not None:
        cards.append(_card("AMB-TEMP", float(temperature_c), "ambient temp [C]"))
    if probe_temp_c is not None:
        cards.append(_card("PRB-TEMP", float(probe_temp_c), "probe temp [C]"))
    cards.append(b"END".ljust(80))  # END has no '=' or value, unlike every other card

    header = b"".join(cards)
    header += b" " * ((-len(header)) % BLOCK)
    data = os.urandom(width * height * 2)
    data += b"\0" * ((-len(data)) % BLOCK)
    return header + data


def fits_dimensions(target_mb: int) -> tuple[int, int]:
    """Square frame whose 16-bit pixel data is about target_mb."""
    side = int((target_mb * 1024 * 1024 / 2) ** 0.5)
    return side, side


# ---------------------------------------------------------------------------
# Synthetic preview. Stars thin out as cloud cover rises, which is exactly what
# detector.py measures, so the scores it produces will move.
# ---------------------------------------------------------------------------
def make_preview(cloud_frac: float, gamma, contrast, brightness) -> bytes:
    from PIL import Image, ImageDraw, ImageEnhance, ImageFilter

    img = Image.new("RGB", (PREVIEW_PX, PREVIEW_PX), (6, 8, 18))
    draw = ImageDraw.Draw(img)

    # Squared, not linear: cloud occludes stars faster than it covers sky, and a
    # linear falloff left even 0.95-cloud frames with ~11 visible stars — enough
    # to score 0.575 against detector.py's 40-star expectation and never trip the
    # 0.6 threshold. A fixture that can't produce a cloudy frame is a bad fixture.
    n_stars = int(220 * (1.0 - cloud_frac) ** 2)
    for _ in range(n_stars):
        x, y = random.uniform(0, PREVIEW_PX), random.uniform(0, PREVIEW_PX)
        r = random.choice([0.6, 0.9, 1.2, 1.8])
        v = random.randint(140, 255)
        draw.ellipse([x - r, y - r, x + r, y + r], fill=(v, v, min(255, v + 10)))

    if cloud_frac > 0.05:
        clouds = Image.new("RGB", (PREVIEW_PX, PREVIEW_PX), (0, 0, 0))
        cd = ImageDraw.Draw(clouds)
        for _ in range(int(14 * cloud_frac) + 1):
            cx, cy = random.uniform(0, PREVIEW_PX), random.uniform(0, PREVIEW_PX)
            rr = random.uniform(PREVIEW_PX * 0.08, PREVIEW_PX * 0.3)
            g = int(70 * cloud_frac) + random.randint(0, 40)
            cd.ellipse([cx - rr, cy - rr * 0.6, cx + rr, cy + rr * 0.6], fill=(g, g, g + 6))
        blurred = clouds.filter(ImageFilter.GaussianBlur(PREVIEW_PX // 40))
        img = Image.blend(img, blurred, cloud_frac)

    # Same order as send.py: gamma, then contrast, then brightness.
    if gamma is not None and gamma > 0:
        lut = [min(255, int(((i / 255.0) ** gamma) * 255.0)) for i in range(256)] * 3
        img = img.point(lut)
    if contrast is not None:
        img = ImageEnhance.Contrast(img).enhance(contrast)
    if brightness is not None:
        img = ImageEnhance.Brightness(img).enhance(max(0.0, 1.0 + brightness / 128.0))

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=85)
    return out.getvalue()


class FakePi:
    def __init__(self, args):
        self.args = args
        self.md = (("authorization", f"Bearer {TOKEN}"),)
        self.stop = threading.Event()
        self.outbox: queue.Queue = queue.Queue(maxsize=64)  # -> DeviceSession
        self.uploads: queue.Queue = queue.Queue()            # -> uploader thread
        # Frames handed to the uploader and not yet stored or dead-lettered.
        # queue.join() can't be used for this: a retry is re-queued after a
        # backoff delay, so the queue is briefly empty while work remains.
        self.pending = 0
        self.pending_cv = threading.Condition()
        self.capture_now = threading.Event()                 # set by CaptureNow
        self.settings_lock = threading.Lock()
        self.settings: dict = {}
        self.attempts: dict = {}
        self.failed_once: set = set()
        self.stats = {"frames": 0, "fits": 0, "uploaded": 0, "resumed": 0, "interrupted": 0,
                      "duplicate": 0, "telemetry": 0, "settings": 0, "commands": 0, "dlq": 0}
        options = [
            # Must be >= the server's KeepaliveEnforcementPolicy.MinTime (20s),
            # or the server closes the connection with ENHANCE_YOUR_CALM.
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_permit_without_calls", 1),
        ]
        if USE_TLS:
            # Real deployments go through Cloudflare -> Traefik, so the device
            # talks TLS to a public hostname and verifies it with the system
            # roots. No client cert: the bearer token is the credential.
            root = None
            if CA_FILE:
                with open(CA_FILE, "rb") as fh:
                    root = fh.read()
            self.channel = grpc.secure_channel(
                TARGET, grpc.ssl_channel_credentials(root_certificates=root),
                options=options)
        else:
            self.channel = grpc.insecure_channel(TARGET, options=options)
        self.stub = rpc.SkycamServiceStub(self.channel)

    def eff(self, key, default):
        with self.settings_lock:
            v = self.settings.get(key)
        return v if v is not None else default

    # -- DeviceSession: the control plane -----------------------------------
    def _session_requests(self):
        """The only producer of outbound session messages: gRPC forbids two
        concurrent Sends on one stream, so everything funnels through here."""
        yield pb.DeviceSessionRequest(hello=pb.Hello(device_id=DEVICE_ID, client_version=CLIENT_VERSION))
        last_beat = time.monotonic()
        while not self.stop.is_set():
            try:
                msg = self.outbox.get(timeout=0.5)
                yield msg
                continue
            except queue.Empty:
                pass
            if time.monotonic() - last_beat >= HEARTBEAT_S:
                last_beat = time.monotonic()
                yield pb.DeviceSessionRequest(heartbeat=pb.Heartbeat(sent_at=now_ts()))

    def session_thread(self):
        backoff = 1.0
        while not self.stop.is_set():
            try:
                log.info("opening DeviceSession to %s as %s", TARGET, DEVICE_ID)
                responses = self.stub.DeviceSession(self._session_requests(), metadata=self.md)
                for resp in responses:          # the single reader of this stream
                    backoff = 1.0
                    if resp.HasField("settings"):
                        self._apply_settings(resp.settings)
                    elif resp.HasField("command"):
                        self._handle_command(resp.command)
            except grpc.RpcError as e:
                if self.stop.is_set():
                    return
                log.warning("session dropped (%s: %s); reconnecting in %.0fs",
                            e.code().name, e.details(), backoff)
                # On reconnect the server re-sends current settings, which is why
                # a lost push is harmless: the DB is the source of truth.
                self.stop.wait(backoff)
                backoff = min(backoff * 2, 30.0)

    def _apply_settings(self, s: pb.DeviceSettings):
        new = {}
        for f in ("exposure_ms", "gain", "preview_gamma", "preview_contrast", "preview_brightness"):
            if s.HasField(f):
                new[f] = getattr(s, f)
        with self.settings_lock:
            self.settings = new
        self.stats["settings"] += 1
        log.info("settings pushed: %s", new or "(all defaults)")

    def _handle_command(self, c: pb.Command):
        self.stats["commands"] += 1
        kind = c.WhichOneof("kind")
        log.info("command received: %s (%s)", kind, c.command_id)
        if kind == "capture_now":
            self.capture_now.set()
        # AbortExposure would cancel an in-flight exposure; there is no real
        # shutter here, so acking is all it can honestly do.
        self.outbox.put(pb.DeviceSessionRequest(
            command_ack=pb.CommandAck(command_id=c.command_id, ok=True)))

    # -- capture: produces spooled frames ----------------------------------
    def capture_thread(self):
        last_fits = 0.0
        cycles = 0
        while not self.stop.is_set():
            start = time.monotonic()
            temperature_c = round(random.uniform(4.0, 19.0), 2)
            humidity_pct = round(random.uniform(35.0, 92.0), 2)
            probe_temp_c = round(temperature_c - random.uniform(0.2, 1.6), 2)

            try:
                self.outbox.put(pb.DeviceSessionRequest(telemetry=pb.Telemetry(
                    recorded_at=now_ts(), temperature_c=temperature_c,
                    humidity_pct=humidity_pct, probe_temp_c=probe_temp_c)), timeout=1.0)
                self.stats["telemetry"] += 1
            except queue.Full:
                # Session is down. Dropping a reading is fine; the next is 2s away.
                log.warning("session outbox full; dropping one telemetry reading")

            want_fits = last_fits == 0.0 or (start - last_fits) >= self.args.fits_every
            self._spool_frame(want_fits, temperature_c, probe_temp_c)
            if want_fits:
                last_fits = start

            cycles += 1
            if self.args.cycles and cycles >= self.args.cycles:
                log.info("captured %d cycles; waiting for uploads to drain", cycles)
                return

            elapsed = time.monotonic() - start
            if self.capture_now.wait(timeout=max(0.0, INTERVAL_S - elapsed)):
                self.capture_now.clear()
                log.info("CaptureNow: capturing off-cycle")

    def _spool_frame(self, want_fits, temperature_c, probe_temp_c):
        """Write the frame to disk FIRST, then hand the id to the uploader. This
        is what send.py is missing: the bytes outlive a failed upload."""
        frame_id = str(uuid.uuid4())
        captured_at = dt.datetime.now(dt.timezone.utc).isoformat()
        exposure_ms = float(self.eff("exposure_ms", EXPOSURE_MS))
        gain = int(self.eff("gain", GAIN))

        # A slow oscillation so scores move instead of sitting at one value.
        cloud_frac = max(0.0, min(1.0, 0.5 + 0.5 * random.uniform(-1, 1) * 0.9))
        preview = make_preview(cloud_frac, self.eff("preview_gamma", None),
                               self.eff("preview_contrast", None),
                               self.eff("preview_brightness", None))

        d = os.path.join(SPOOL_DIR, frame_id)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "preview.jpg"), "wb") as fh:
            fh.write(preview)

        meta = {"frame_id": frame_id, "device_id": DEVICE_ID, "captured_at": captured_at,
                "temperature_c": temperature_c, "probe_temp_c": probe_temp_c,
                "exposure_ms": exposure_ms, "gain": gain, "fits": False}
        if want_fits:
            w, h = fits_dimensions(self.args.fits_mb)
            fits = make_fits(w, h, captured_at, exposure_ms, gain, temperature_c, probe_temp_c)
            with open(os.path.join(d, "frame.fits"), "wb") as fh:
                fh.write(fits)
            meta.update(fits=True, fits_size=len(fits),
                        fits_sha256=hashlib.sha256(fits).hexdigest())
            self.stats["fits"] += 1

        # Written last: the uploader treats meta.json as the commit marker, so a
        # crash mid-write leaves a directory it will ignore rather than a
        # half-written frame it would try to send.
        with open(os.path.join(d, "meta.json"), "w") as fh:
            json.dump(meta, fh)

        self.stats["frames"] += 1
        log.info("spooled %s  preview=%dKB%s", frame_id[:8], len(preview) // 1024,
                 f"  fits={meta['fits_size'] // (1024 * 1024)}MB" if want_fits else "")
        self.enqueue(frame_id)

    # -- uploader: drains the spool ----------------------------------------
    def enqueue(self, frame_id):
        with self.pending_cv:
            self.pending += 1
        self.uploads.put(frame_id)

    def _settle(self):
        """One frame reached a terminal state: stored, duplicate, or DLQ."""
        with self.pending_cv:
            self.pending -= 1
            self.pending_cv.notify_all()

    def drain(self, timeout=180.0):
        deadline = time.monotonic() + timeout
        with self.pending_cv:
            while self.pending > 0 and time.monotonic() < deadline:
                self.pending_cv.wait(timeout=1.0)
            return self.pending == 0

    def uploader_thread(self):
        while True:
            frame_id = self.uploads.get()
            if frame_id is None:
                return
            try:
                self._upload(frame_id)
                self.attempts.pop(frame_id, None)
                self._settle()
            except grpc.RpcError as e:
                n = self.attempts.get(frame_id, 0) + 1
                self.attempts[frame_id] = n
                log.warning("upload %s failed (%s), attempt %d/%d",
                            frame_id[:8], e.code().name, n, UPLOAD_ATTEMPTS)
                if n >= UPLOAD_ATTEMPTS:
                    self._to_dlq(frame_id)
                    self._settle()
                else:
                    # Re-queued at the BACK after a backoff, not retried in
                    # place: one bad frame must not block the good ones behind
                    # it. It stays counted in `pending` the whole time.
                    threading.Timer(min(2 ** n, 30), self.uploads.put,
                                    args=(frame_id,)).start()
            except Exception:
                log.exception("upload %s hit a non-RPC error; to DLQ", frame_id[:8])
                self._to_dlq(frame_id)
                self._settle()

    def _to_dlq(self, frame_id):
        dead = os.path.join(SPOOL_DIR, "failed")
        os.makedirs(dead, exist_ok=True)
        shutil.move(os.path.join(SPOOL_DIR, frame_id), os.path.join(dead, frame_id))
        self.stats["dlq"] += 1
        log.error("frame %s moved to spool/failed after %d attempts", frame_id[:8], UPLOAD_ATTEMPTS)

    def _upload(self, frame_id):
        d = os.path.join(SPOOL_DIR, frame_id)
        with open(os.path.join(d, "meta.json")) as fh:
            meta = json.load(fh)

        offset = 0
        if meta["fits"]:
            st = self.stub.GetUploadStatus(
                pb.GetUploadStatusRequest(frame_id=frame_id, device_id=DEVICE_ID),
                metadata=self.md, timeout=30)
            if st.state == pb.UPLOAD_STATE_COMPLETE:
                log.info("upload %s already complete server-side; dropping spool", frame_id[:8])
                shutil.rmtree(d)
                self.stats["duplicate"] += 1
                return
            if st.state == pb.UPLOAD_STATE_IN_PROGRESS:
                offset = st.committed_bytes
                self.stats["resumed"] += 1
                log.info("resuming %s from %.1fMB of %.1fMB", frame_id[:8],
                         offset / 1048576, meta["fits_size"] / 1048576)

        resp = self.stub.UploadFrame(self._upload_requests(d, meta, offset),
                                     metadata=self.md, timeout=600)
        self.stats["uploaded"] += 1
        log.info("stored %s  duplicate=%s", resp.frame_id[:8], resp.duplicate)
        shutil.rmtree(d)

    def _upload_requests(self, d, meta, offset):
        """Header first, then FITS chunks at strictly contiguous offsets starting
        at `offset` — the server rejects any other offset and tells you to call
        GetUploadStatus."""
        with open(os.path.join(d, "preview.jpg"), "rb") as fh:
            preview = fh.read()

        ts = Timestamp()
        ts.FromDatetime(dt.datetime.fromisoformat(meta["captured_at"]))
        header = pb.FrameHeader(frame_id=meta["frame_id"], device_id=meta["device_id"],
                                captured_at=ts, preview_jpeg=preview,
                                temperature_c=meta["temperature_c"],
                                probe_temp_c=meta["probe_temp_c"])
        if meta["fits"]:
            # sha256 is of the WHOLE file, not the resumed tail: the server
            # verifies the assembled object, including parts from earlier attempts.
            header.fits.CopyFrom(pb.FitsInfo(size_bytes=meta["fits_size"],
                                             sha256=bytes.fromhex(meta["fits_sha256"])))
        yield pb.UploadFrameRequest(header=header)

        if not meta["fits"]:
            return

        fail_at = self.args.fail_upload_at_mb * 1048576 if self.args.fail_upload_at_mb else 0
        # Bytes per second, or 0 for as-fast-as-possible. Throttling models a
        # real uplink, and it also keeps the client from running so far ahead of
        # the server that an injected failure lands before the server has
        # committed any 5MB part — in which case there is nothing to resume from.
        rate = self.args.throttle_mbps * 1048576 / 8.0 if self.args.throttle_mbps else 0
        sent = 0
        with open(os.path.join(d, "frame.fits"), "rb") as fh:
            fh.seek(offset)
            pos = offset
            while True:
                data = fh.read(CHUNK_BYTES)
                if not data:
                    return
                if fail_at and meta["frame_id"] not in self.failed_once and sent + len(data) > fail_at:
                    self.failed_once.add(meta["frame_id"])
                    self.stats["interrupted"] += 1
                    log.warning("FAULT INJECTION: dropping %s at %.1fMB",
                                meta["frame_id"][:8], pos / 1048576)
                    raise RuntimeError("injected network failure")
                yield pb.UploadFrameRequest(chunk=pb.FitsChunk(offset=pos, data=data))
                pos += len(data)
                sent += len(data)
                if rate:
                    time.sleep(len(data) / rate)

    def recover_spool(self):
        """Frames left by a previous run: upload them before capturing new ones."""
        if not os.path.isdir(SPOOL_DIR):
            return
        pending = sorted(n for n in os.listdir(SPOOL_DIR)
                         if os.path.isfile(os.path.join(SPOOL_DIR, n, "meta.json")))
        for frame_id in pending:
            log.info("recovered %s from spool", frame_id[:8])
            self.enqueue(frame_id)
        if pending:
            log.info("recovered %d frame(s) from a previous run", len(pending))

    def run(self):
        os.makedirs(SPOOL_DIR, exist_ok=True)
        self.recover_spool()
        threads = [
            threading.Thread(target=self.session_thread, name="session", daemon=True),
            threading.Thread(target=self.uploader_thread, name="uploader", daemon=True),
            threading.Thread(target=self.capture_thread, name="capture", daemon=True),
        ]
        for t in threads:
            t.start()
        try:
            threads[2].join()          # capture finishes first in --cycles mode
            if not self.drain():
                log.warning("gave up waiting; %d frame(s) still queued", self.pending)
        except KeyboardInterrupt:
            log.info("interrupted")
        finally:
            self.stop.set()
            self.uploads.put(None)
            time.sleep(0.3)
            self.channel.close()
        log.info("stats: %s", json.dumps(self.stats))
        return 0 if self.stats["dlq"] == 0 else 1


def main():
    p = argparse.ArgumentParser(description="fake sky camera speaking gRPC to the Go ingest service")
    p.add_argument("--cycles", type=int, default=0, help="stop after N captures (0 = forever)")
    p.add_argument("--fits-every", type=float, default=FITS_INTERVAL_S, help="seconds between FITS uploads")
    p.add_argument("--fits-mb", type=int, default=FITS_MB, help="synthetic FITS size in MB")
    p.add_argument("--fail-upload-at-mb", type=float, default=0.0,
                   help="drop each FITS upload once at this many MB, to demo resume")
    p.add_argument("--throttle-mbps", type=float, default=0.0,
                   help="pace the FITS upload to this many Mbit/s (0 = unlimited)")
    args = p.parse_args()

    if not TOKEN:
        log.error("OBS_INGEST_TOKEN is not set — refusing to run")
        return 1
    if args.fail_upload_at_mb:
        # grpc logs the request-iterator exception at ERROR with a traceback,
        # which is just noise when the failure is the point of the run.
        logging.getLogger("grpc").setLevel(logging.CRITICAL)
    log.info("target %s  device %s  interval %.1fs  fits every %.0fs (%dMB)",
             TARGET, DEVICE_ID, INTERVAL_S, args.fits_every, args.fits_mb)
    return FakePi(args).run()


if __name__ == "__main__":
    sys.exit(main())
