package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/testenv"
)

func TestInsertIsIdempotentAndNotifiesOnce(t *testing.T) {
	env := testenv.SetupDB(t)
	frames := store.NewPostgres(env.Pool)
	listener := env.Listen(t)
	ctx := context.Background()

	preview := "previews/skycam/x.jpg"
	f := store.Frame{ID: uuid.New(), DeviceID: "skycam", CapturedAt: time.Now().UTC(), PreviewKey: &preview}

	inserted, err := frames.Insert(ctx, f)
	if err != nil || !inserted {
		t.Fatalf("first insert: inserted=%v err=%v", inserted, err)
	}
	if !listener.WaitFor(f.ID.String(), 5*time.Second) {
		t.Fatal("no NOTIFY after first insert")
	}

	inserted, err = frames.Insert(ctx, f)
	if err != nil || inserted {
		t.Fatalf("second insert: inserted=%v err=%v, want no-op", inserted, err)
	}
	if listener.WaitFor(f.ID.String(), time.Second) {
		t.Fatal("duplicate insert sent a second NOTIFY")
	}

	if ok, err := frames.Exists(ctx, f.ID); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
}

func TestFailedInsertSendsNoNotify(t *testing.T) {
	env := testenv.SetupDB(t)
	frames := store.NewPostgres(env.Pool)
	listener := env.Listen(t)

	// fits_key without size/sha violates the fits_fields_together constraint,
	// so the transaction rolls back — and the NOTIFY with it.
	key := "frames/skycam/x.fits"
	f := store.Frame{ID: uuid.New(), DeviceID: "skycam", CapturedAt: time.Now().UTC(), FitsKey: &key}
	if _, err := frames.Insert(context.Background(), f); err == nil {
		t.Fatal("expected constraint violation")
	}
	if listener.WaitFor(f.ID.String(), time.Second) {
		t.Fatal("rolled-back insert still sent a NOTIFY")
	}
}
