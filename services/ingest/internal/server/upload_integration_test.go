package server

import (
	"context"
	"crypto/sha256"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/blob"
)

// Real FITS size from the ZWO ASI676MC: 4 full 5MiB parts plus a short one.
const fitsSize = 26_000_000

const chunkSize = 256 << 10

type frame struct {
	id      uuid.UUID
	fits    []byte
	sum     [32]byte
	preview []byte
}

func newFrame(fitsBytes int) frame {
	r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	f := frame{id: uuid.New(), preview: make([]byte, 50_000)}
	for i := range f.preview {
		f.preview[i] = byte(r.Uint32())
	}
	if fitsBytes > 0 {
		f.fits = make([]byte, fitsBytes)
		for i := range f.fits {
			f.fits[i] = byte(r.Uint32())
		}
		f.sum = sha256.Sum256(f.fits)
	}
	return f
}

func (f frame) header(deviceID string) *skycamv1.FrameHeader {
	h := &skycamv1.FrameHeader{
		FrameId:     f.id.String(),
		DeviceId:    deviceID,
		CapturedAt:  timestamppb.Now(),
		PreviewJpeg: f.preview,
	}
	if f.fits != nil {
		h.Fits = &skycamv1.FitsInfo{SizeBytes: uint64(len(f.fits)), Sha256: f.sum[:]}
	}
	return h
}

// send uploads header + FITS bytes [from, to). If finish is false the stream is
// cancelled mid-flight instead of closed, simulating a dropped connection.
func (h *harness) send(t *testing.T, f frame, hdr *skycamv1.FrameHeader, from, to int64, finish bool) (*skycamv1.UploadFrameResponse, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := h.client.UploadFrame(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&skycamv1.UploadFrameRequest{Payload: &skycamv1.UploadFrameRequest_Header{Header: hdr}}); err != nil {
		return nil, err
	}
	for off := from; off < to; off += chunkSize {
		end := min(off+chunkSize, to)
		err := stream.Send(&skycamv1.UploadFrameRequest{Payload: &skycamv1.UploadFrameRequest_Chunk{
			Chunk: &skycamv1.FitsChunk{Offset: uint64(off), Data: f.fits[off:end]},
		}})
		if err == io.EOF {
			break // server already answered; CloseAndRecv returns why
		}
		if err != nil {
			return nil, err
		}
	}
	if !finish {
		cancel()
		return nil, nil
	}
	return stream.CloseAndRecv()
}

// resume uploads whatever GetUploadStatus says is missing, retrying while the
// server still holds the previous (cancelled) stream's lock — as a real client would.
func (h *harness) resume(t *testing.T, f frame, hdr *skycamv1.FrameHeader) (*skycamv1.UploadFrameResponse, uint64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		st, err := h.client.GetUploadStatus(context.Background(),
			&skycamv1.GetUploadStatusRequest{FrameId: f.id.String(), DeviceId: hdr.GetDeviceId()})
		if err != nil {
			t.Fatalf("GetUploadStatus: %v", err)
		}
		resp, err := h.send(t, f, hdr, int64(st.GetCommittedBytes()), int64(len(f.fits)), true)
		if status.Code(err) == codes.Aborted && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("resume from %d: %v", st.GetCommittedBytes(), err)
		}
		return resp, st.GetCommittedBytes()
	}
}

func (h *harness) assertStored(t *testing.T, f frame) {
	t.Helper()
	ctx := context.Background()
	got, err := h.env.Blobs.SHA256(ctx, fitsKey(h.device, f.id))
	if err != nil {
		t.Fatalf("read back fits: %v", err)
	}
	if [32]byte(got) != f.sum {
		t.Fatal("stored FITS checksum differs from the original")
	}
	var size int64
	var status string
	err = h.env.Pool.QueryRow(ctx,
		`SELECT fits_size_bytes, detect_status FROM frames WHERE frame_id = $1`, f.id.String()).Scan(&size, &status)
	if err != nil {
		t.Fatalf("frame row: %v", err)
	}
	if size != int64(len(f.fits)) || status != "pending" {
		t.Fatalf("row: size=%d status=%s", size, status)
	}
}

func TestUploadFrameRoundTrip(t *testing.T) {
	h := newHarness(t)
	listener := h.env.Listen(t)
	f := newFrame(fitsSize)
	hdr := f.header(h.device)

	resp, err := h.send(t, f, hdr, 0, fitsSize, true)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if resp.GetDuplicate() {
		t.Fatal("first upload reported duplicate")
	}
	h.assertStored(t, f)
	if !listener.WaitFor(f.id.String(), 5*time.Second) {
		t.Fatal("no NOTIFY for committed frame")
	}

	st, err := h.client.GetUploadStatus(context.Background(),
		&skycamv1.GetUploadStatusRequest{FrameId: f.id.String(), DeviceId: h.device})
	if err != nil || st.GetState() != skycamv1.UploadState_UPLOAD_STATE_COMPLETE {
		t.Fatalf("status after commit = %v, %v; want COMPLETE", st.GetState(), err)
	}

	// Retrying the same frame is a no-op: duplicate, and no second event.
	resp, err = h.send(t, f, hdr, 0, 0, true)
	if err != nil || !resp.GetDuplicate() {
		t.Fatalf("retry: duplicate=%v err=%v", resp.GetDuplicate(), err)
	}
	if listener.WaitFor(f.id.String(), time.Second) {
		t.Fatal("duplicate upload sent a second NOTIFY")
	}
}

// M2's done criterion: cut the connection at ~80% of a 25MB upload, reconnect,
// resume from the last completed part, checksum matches.
func TestUploadFrameResumesAfterDrop(t *testing.T) {
	h := newHarness(t)
	f := newFrame(fitsSize)
	hdr := f.header(h.device)
	cut := int64(fitsSize * 8 / 10) // 20.8MB

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := h.client.UploadFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&skycamv1.UploadFrameRequest{Payload: &skycamv1.UploadFrameRequest_Header{Header: hdr}}); err != nil {
		t.Fatal(err)
	}
	for off := int64(0); off < cut; off += chunkSize {
		end := min(off+chunkSize, cut)
		if err := stream.Send(&skycamv1.UploadFrameRequest{Payload: &skycamv1.UploadFrameRequest_Chunk{
			Chunk: &skycamv1.FitsChunk{Offset: uint64(off), Data: f.fits[off:end]},
		}}); err != nil {
			t.Fatalf("send at %d: %v", off, err)
		}
	}
	// HTTP/2 flow control lets the client run megabytes ahead of the server, so
	// wait until the server has durably stored parts 1-3 (15MiB) before cutting.
	// Part 4 would end at 20.97MB, past the cut, so it can never complete.
	want := uint64(3 * blob.PartSize)
	waitUntil(t, 20*time.Second, func() bool {
		st, err := h.client.GetUploadStatus(context.Background(),
			&skycamv1.GetUploadStatusRequest{FrameId: f.id.String(), DeviceId: hdr.GetDeviceId()})
		return err == nil && st.GetCommittedBytes() >= want
	})
	cancel() // the connection drops with part 4 half-buffered

	resp, resumedFrom := h.resume(t, f, hdr)
	if resp.GetDuplicate() {
		t.Fatal("resumed upload reported duplicate")
	}
	if resumedFrom != want {
		t.Fatalf("resumed from %d, want %d (3 full parts; the partial 4th is re-sent)", resumedFrom, want)
	}
	h.assertStored(t, f)
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestUploadFrameRejectsWrongOffset(t *testing.T) {
	h := newHarness(t)
	f := newFrame(fitsSize)
	_, err := h.send(t, f, f.header(h.device), 1000, 2000, true)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestUploadFrameChecksumMismatchStoresNothing(t *testing.T) {
	h := newHarness(t)
	f := newFrame(6 << 20)
	hdr := f.header(h.device)
	hdr.Fits.Sha256 = make([]byte, 32) // wrong

	_, err := h.send(t, f, hdr, 0, int64(len(f.fits)), true)
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("got %v, want DataLoss", err)
	}
	if _, exists, err := h.env.Blobs.Stat(context.Background(), fitsKey(h.device, f.id)); err != nil || exists {
		t.Fatalf("corrupt object still stored: exists=%v err=%v", exists, err)
	}
	st, err := h.client.GetUploadStatus(context.Background(),
		&skycamv1.GetUploadStatusRequest{FrameId: f.id.String(), DeviceId: h.device})
	if err != nil || st.GetState() != skycamv1.UploadState_UPLOAD_STATE_NOT_FOUND {
		t.Fatalf("status = %v, %v; want NOT_FOUND so the client restarts at 0", st.GetState(), err)
	}
}

// Crash window: the object was assembled but the row was never committed.
// The retry sends no chunks; the server verifies the object and commits.
func TestUploadFrameCommitsAlreadyAssembledObject(t *testing.T) {
	h := newHarness(t)
	f := newFrame(6 << 20)
	ctx := context.Background()

	up, err := h.env.Blobs.Resume(ctx, fitsKey(h.device, f.id), "image/fits", int64(len(f.fits)))
	if err != nil {
		t.Fatal(err)
	}
	if err := up.WritePart(ctx, f.fits[:blob.PartSize]); err != nil {
		t.Fatal(err)
	}
	if err := up.WritePart(ctx, f.fits[blob.PartSize:]); err != nil {
		t.Fatal(err)
	}
	if err := up.Complete(ctx); err != nil {
		t.Fatal(err)
	}

	resp, resumedFrom := h.resume(t, f, f.header(h.device))
	if resumedFrom != uint64(len(f.fits)) || resp.GetDuplicate() {
		t.Fatalf("resumedFrom=%d duplicate=%v; want full size, not duplicate", resumedFrom, resp.GetDuplicate())
	}
	h.assertStored(t, f)
}

func TestPreviewOnlyFrame(t *testing.T) {
	h := newHarness(t)
	f := newFrame(0)
	resp, err := h.send(t, f, f.header(h.device), 0, 0, true)
	if err != nil || resp.GetDuplicate() {
		t.Fatalf("upload: duplicate=%v err=%v", resp.GetDuplicate(), err)
	}
	var fitsKey *string
	var status string
	err = h.env.Pool.QueryRow(context.Background(),
		`SELECT fits_key, detect_status FROM frames WHERE frame_id = $1`, f.id.String()).Scan(&fitsKey, &status)
	if err != nil || fitsKey != nil || status != "pending" {
		t.Fatalf("row: fits_key=%v status=%s err=%v", fitsKey, status, err)
	}
}

func TestUploadFrameValidatesHeader(t *testing.T) {
	h := newHarness(t)
	base := newFrame(0)
	cases := map[string]func(*skycamv1.FrameHeader){
		"frame_id not a uuid": func(h *skycamv1.FrameHeader) { h.FrameId = "abc" },

		"missing captured_at": func(h *skycamv1.FrameHeader) { h.CapturedAt = nil },
		"nothing to store":    func(h *skycamv1.FrameHeader) { h.PreviewJpeg = nil },
		"bad sha length": func(h *skycamv1.FrameHeader) {
			h.Fits = &skycamv1.FitsInfo{SizeBytes: 10, Sha256: []byte{1}}
		},
	}
	// A forged device_id is refused outright, which is also why a path-escaping
	// one can never reach the key builder.
	t.Run("device_id of another camera", func(t *testing.T) {
		hdr := base.header(h.device)
		hdr.DeviceId = "../etc"
		if _, err := h.send(t, base, hdr, 0, 0, true); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("got %v, want PermissionDenied", err)
		}
	})

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			hdr := base.header(h.device)
			mutate(hdr)
			_, err := h.send(t, base, hdr, 0, 0, true)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("got %v, want InvalidArgument", err)
			}
		})
	}
}
