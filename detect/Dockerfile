# syntax=docker/dockerfile:1.7
#
# Cloud-detection Celery app. One image, two roles, chosen by the command:
#   worker: celery -A celery_app worker --loglevel=info
#   beat:   celery -A celery_app beat   --loglevel=info
# DETECT_BACKEND=postgres selects the Go-ingest pipeline (tasks_pg.py).
#
# Behind a TLS-intercepting proxy:
#   docker build --secret id=extra_ca,src=corp-ca-bundle.crt -t detect detect

FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN --mount=type=secret,id=extra_ca,required=false \
    if [ -s /run/secrets/extra_ca ]; then \
      export PIP_CERT=/tmp/ca.pem; \
      cat /etc/ssl/certs/ca-certificates.crt /run/secrets/extra_ca > /tmp/ca.pem; \
    fi; \
    pip install -r requirements.txt && rm -f /tmp/ca.pem
COPY . .
# Celery refuses to run as root without C_FORCE_ROOT, and should.
RUN useradd --uid 10001 --no-create-home app
USER 10001
ENV DETECT_BACKEND=postgres
CMD ["celery", "-A", "celery_app", "worker", "--loglevel=info"]
