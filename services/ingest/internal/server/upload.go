package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/blob"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

// device_id becomes part of object keys, so it must not contain '/' or '..'.
var deviceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func fitsKey(deviceID string, id uuid.UUID) string {
	return fmt.Sprintf("frames/%s/%s.fits", deviceID, id)
}

func previewKey(deviceID string, id uuid.UUID) string {
	return fmt.Sprintf("previews/%s/%s.jpg", deviceID, id)
}

// UploadFrame receives one frame: a header (with the inline preview), then the
// FITS in chunks starting at the offset GetUploadStatus reported.
//
// Order of durable writes: blobs first, then the metadata row + NOTIFY in one
// transaction. A failure between them leaves an orphaned blob (harmless; the
// retry reuses it). The reverse order could leave a row pointing at nothing.
func (s *Server) UploadFrame(stream skycamv1.SkycamService_UploadFrameServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "expected a FrameHeader first")
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first message must be a FrameHeader")
	}
	deviceID, ok := auth.DeviceFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "device authentication required")
	}
	f, err := s.parseHeader(hdr, deviceID)
	if err != nil {
		return err
	}

	unlock, ok := s.inflight.tryLock(f.ID.String())
	if !ok {
		return status.Error(codes.Aborted, "an upload for this frame is already in progress; retry shortly")
	}
	defer unlock()

	exists, err := s.frames.Exists(ctx, f.ID)
	if err != nil {
		return s.unavailable("check frame", err)
	}
	if exists {
		return stream.SendAndClose(&skycamv1.UploadFrameResponse{FrameId: f.ID.String(), Duplicate: true})
	}

	if len(hdr.GetPreviewJpeg()) > 0 {
		key := previewKey(f.DeviceID, f.ID)
		if err := s.blobs.PutObject(ctx, key, "image/jpeg", hdr.GetPreviewJpeg()); err != nil {
			return s.unavailable("store preview", err)
		}
		f.PreviewKey = &key
	}

	if fits := hdr.GetFits(); fits != nil {
		if err := s.receiveFits(ctx, stream, &f, fits); err != nil {
			return err
		}
	} else if err := expectEOF(stream); err != nil {
		return err
	}

	inserted, err := s.frames.Insert(ctx, f)
	if err != nil {
		return s.unavailable("commit frame", err)
	}
	s.log.Info("frame stored", "frame_id", f.ID, "device_id", f.DeviceID,
		"fits", f.FitsKey != nil, "inserted", inserted)
	return stream.SendAndClose(&skycamv1.UploadFrameResponse{FrameId: f.ID.String(), Duplicate: !inserted})
}

// receiveFits streams the FITS into a resumable multipart upload, completes it,
// and verifies the SHA-256 of the assembled object.
func (s *Server) receiveFits(ctx context.Context, stream skycamv1.SkycamService_UploadFrameServer,
	f *store.Frame, fits *skycamv1.FitsInfo) error {

	total := int64(fits.GetSizeBytes())
	key := fitsKey(f.DeviceID, f.ID)

	size, alreadyStored, err := s.blobs.Stat(ctx, key)
	if err != nil {
		return s.unavailable("stat fits", err)
	}

	if alreadyStored {
		// A previous attempt finished the object but didn't commit the row.
		// The client resumes at committed_bytes == size, so it sends no chunks.
		if size != total {
			return status.Errorf(codes.FailedPrecondition,
				"stored FITS is %d bytes but header declares %d; use a new frame_id", size, total)
		}
		if err := expectEOF(stream); err != nil {
			return err
		}
	} else {
		up, err := s.blobs.Resume(ctx, key, "image/fits", total)
		if errors.Is(err, blob.ErrSizeConflict) {
			return status.Error(codes.FailedPrecondition, "stored parts don't match declared size; use a new frame_id")
		}
		if err != nil {
			return s.unavailable("start fits upload", err)
		}

		next := up.Committed()
		parts := &partBuffer{size: blob.PartSize, flush: func(b []byte) error { return up.WritePart(ctx, b) }}
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				// Stream broke (client gone, deadline, network). Flushed parts
				// stay in S3; buffered bytes are dropped. Client resumes later.
				s.log.Info("fits upload interrupted", "frame_id", f.ID, "committed", up.Committed(), "err", err)
				return err
			}
			c := msg.GetChunk()
			if c == nil {
				return status.Error(codes.InvalidArgument, "only FitsChunks may follow the header")
			}
			if int64(c.GetOffset()) != next {
				return status.Errorf(codes.FailedPrecondition,
					"expected chunk at offset %d, got %d (call GetUploadStatus to resume)", next, c.GetOffset())
			}
			if next+int64(len(c.GetData())) > total {
				return status.Errorf(codes.InvalidArgument, "chunks exceed declared size %d", total)
			}
			if err := parts.write(c.GetData()); err != nil {
				return s.unavailable("write fits part", err)
			}
			next += int64(len(c.GetData()))
		}

		if next != total {
			return status.Errorf(codes.FailedPrecondition,
				"stream ended at %d of %d bytes; resume from %d", next, total, up.Committed())
		}
		if err := parts.finish(); err != nil {
			return s.unavailable("write final fits part", err)
		}
		if err := up.Complete(ctx); err != nil {
			return s.unavailable("complete fits upload", err)
		}
	}

	// End-to-end check over the whole object, including parts sent on earlier
	// connections. Costs one extra read of the file from object storage.
	got, err := s.blobs.SHA256(ctx, key)
	if err != nil {
		return s.unavailable("verify fits", err)
	}
	if !bytes.Equal(got, fits.GetSha256()) {
		// Don't keep a file we know is wrong; the client must re-upload from 0.
		if err := s.blobs.Delete(ctx, key); err != nil {
			s.log.Error("delete corrupt fits failed", "key", key, "err", err)
		}
		return status.Error(codes.DataLoss, "FITS checksum mismatch; re-upload from offset 0")
	}

	f.FitsKey = &key
	f.FitsSizeBytes = &total
	f.FitsSHA256 = fits.GetSha256()
	return nil
}

// GetUploadStatus tells a client where to resume.
func (s *Server) GetUploadStatus(ctx context.Context, req *skycamv1.GetUploadStatusRequest) (*skycamv1.GetUploadStatusResponse, error) {
	id, err := uuid.Parse(req.GetFrameId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "frame_id must be a UUID")
	}
	deviceID, ok := auth.DeviceFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "device authentication required")
	}
	if claimed := req.GetDeviceId(); claimed != "" && claimed != deviceID {
		return nil, status.Error(codes.PermissionDenied, "device_id does not match the authenticated device")
	}

	exists, err := s.frames.Exists(ctx, id)
	if err != nil {
		return nil, s.unavailable("check frame", err)
	}
	if exists {
		return &skycamv1.GetUploadStatusResponse{State: skycamv1.UploadState_UPLOAD_STATE_COMPLETE}, nil
	}

	p, err := s.blobs.Progress(ctx, fitsKey(deviceID, id))
	if err != nil {
		return nil, s.unavailable("check upload progress", err)
	}
	// p.Complete without a row means the object landed but the commit didn't:
	// report it as in progress at its full size, and the retry just commits.
	if p.Complete || p.Committed > 0 {
		return &skycamv1.GetUploadStatusResponse{
			State:          skycamv1.UploadState_UPLOAD_STATE_IN_PROGRESS,
			CommittedBytes: uint64(p.Committed),
		}, nil
	}
	return &skycamv1.GetUploadStatusResponse{State: skycamv1.UploadState_UPLOAD_STATE_NOT_FOUND}, nil
}

// parseHeader validates the header against the authenticated device. The
// device_id in the message is only accepted when it agrees with the token, so a
// stolen token still cannot write frames as another camera.
func (s *Server) parseHeader(h *skycamv1.FrameHeader, deviceID string) (store.Frame, error) {
	id, err := uuid.Parse(h.GetFrameId())
	if err != nil {
		return store.Frame{}, status.Error(codes.InvalidArgument, "frame_id must be a UUID")
	}
	if !deviceIDPattern.MatchString(deviceID) {
		return store.Frame{}, status.Error(codes.Internal, "authenticated device_id is not usable as a key")
	}
	if claimed := h.GetDeviceId(); claimed != "" && claimed != deviceID {
		return store.Frame{}, status.Error(codes.PermissionDenied, "device_id does not match the authenticated device")
	}
	if h.GetCapturedAt() == nil {
		return store.Frame{}, status.Error(codes.InvalidArgument, "captured_at is required")
	}
	if n := len(h.GetPreviewJpeg()); n > s.limits.MaxPreviewBytes {
		return store.Frame{}, status.Errorf(codes.InvalidArgument, "preview is %d bytes, limit %d", n, s.limits.MaxPreviewBytes)
	}
	if fits := h.GetFits(); fits != nil {
		if fits.GetSizeBytes() == 0 || int64(fits.GetSizeBytes()) > s.limits.MaxFitsBytes {
			return store.Frame{}, status.Errorf(codes.InvalidArgument,
				"fits size must be 1..%d bytes", s.limits.MaxFitsBytes)
		}
		if len(fits.GetSha256()) != 32 {
			return store.Frame{}, status.Error(codes.InvalidArgument, "fits sha256 must be 32 bytes")
		}
	}
	if len(h.GetPreviewJpeg()) == 0 && h.GetFits() == nil {
		return store.Frame{}, status.Error(codes.InvalidArgument, "frame has neither a preview nor a FITS")
	}

	return store.Frame{
		ID:           id,
		DeviceID:     deviceID,
		CapturedAt:   h.GetCapturedAt().AsTime(),
		TemperatureC: h.TemperatureC,
		ProbeTempC:   h.ProbeTempC,
	}, nil
}

// expectEOF is used when no chunks should follow the header.
func expectEOF(stream skycamv1.SkycamService_UploadFrameServer) error {
	_, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return status.Error(codes.InvalidArgument, "unexpected chunk: no FITS data expected for this frame")
}

// unavailable logs the underlying error and returns a retryable status without
// leaking storage/database details to the client.
func (s *Server) unavailable(what string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	s.log.Error(what+" failed", "err", err)
	return status.Errorf(codes.Unavailable, "%s failed; retry", what)
}
