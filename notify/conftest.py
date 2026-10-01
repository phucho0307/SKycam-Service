"""Test environment, applied before any test module is imported.

pytest imports conftest first, which is the only reliable place for these: a test
module setting os.environ at its own top level is too late if another module was
imported first and already read the variable.
"""
import os

os.environ["DATABASE_URL"] = os.environ.get(
    "NOTIFY_TEST_DATABASE_URL",
    "postgres://postgres:postgres@localhost:55432/skycam_notify_test",
)
# Tests publish real Celery messages, so they get their own Redis database.
os.environ["REDIS_URL"] = os.environ.get("NOTIFY_TEST_REDIS_URL", "redis://localhost:6379/14")
os.environ.setdefault("NOTIFY_PROVIDER", "stub")
os.environ.setdefault("NOTIFY_MAILER", "fake")
