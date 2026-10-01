"""Test environment, applied before any test module is imported.

pytest imports conftest.py first, which is the only reliable place to set these:
a test module that sets `os.environ` at its own top level is too late if another
test module was imported before it and already read the variable.

Both values deliberately point at throwaway resources. The fixtures truncate
`frames` and publish real Celery messages, so pointing them at the demo database
or at the broker a worker is reading would corrupt both.
"""
import os

os.environ["DATABASE_URL"] = os.environ.get(
    "DETECT_TEST_DATABASE_URL",
    "postgres://postgres:postgres@localhost:55432/skycam_detect_test",
)
os.environ["REDIS_URL"] = os.environ.get(
    "DETECT_TEST_REDIS_URL", "redis://localhost:6379/15"
)
os.environ.setdefault("DETECT_BACKEND", "postgres")
