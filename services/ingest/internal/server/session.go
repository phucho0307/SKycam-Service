package server

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/cluster"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/session"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

// DeviceSession is the long-lived control channel. The device opens it (so NAT
// is irrelevant), sends telemetry and heartbeats up, and receives settings and
// commands down.
//
// Two goroutines per session: one receiving, one sending. That split is forced
// by gRPC — concurrent Send calls on one stream are not allowed — and it is why
// every outbound message goes through the session's queues rather than touching
// the stream directly.
func (s *Server) DeviceSession(stream skycamv1.SkycamService_DeviceSessionServer) error {
	deviceID, ok := auth.DeviceFromContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "device authentication required")
	}

	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "expected a Hello first")
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first message must be a Hello")
	}
	if id := hello.GetDeviceId(); id != "" && id != deviceID {
		return status.Error(codes.PermissionDenied, "device_id does not match the authenticated device")
	}

	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)

	sess := session.New(deviceID, cancel)
	if replaced := s.sessions.Add(sess); replaced {
		s.log.Info("replaced an older session", "device_id", deviceID)
	}
	defer s.sessions.Remove(sess)
	s.log.Info("device connected", "device_id", deviceID, "client", hello.GetClientVersion())

	// Announce to the other replicas that this process holds the stream, and
	// forward anything they route here. Without this the registry is per-process
	// and a second replica cannot reach this device at all.
	//
	// A bus failure is logged, not fatal: the device is connected and its frames
	// and telemetry work regardless. Degrading to "commands only work on this
	// replica" beats refusing the session.
	if s.bus != nil {
		// Subscribe BEFORE announcing. The other order leaves a window where the
		// presence key names this replica but its subscription is not yet live: a
		// command published in that window is delivered to whoever *is*
		// subscribed — possibly the stale replica the device just moved off — and
		// the caller then waits out its ack timeout against a dead stream.
		// Found by the test that moves a device between replicas.
		if err := s.forwardBusCommands(ctx, deviceID, sess); err != nil {
			s.log.Error("cluster command subscription failed; cross-replica commands unavailable",
				"device_id", deviceID, "err", err)
		} else if release, err := s.bus.Announce(ctx, deviceID); err != nil {
			s.log.Error("cluster announce failed; cross-replica commands unavailable",
				"device_id", deviceID, "err", err)
		} else {
			defer release()
		}
	}

	// Send current settings immediately. This is what makes a lost push
	// harmless: the database is the source of truth, and every reconnect
	// re-syncs, so a device can never be left running stale settings.
	if cur, err := s.settings.GetSettings(ctx, deviceID); err != nil {
		s.log.Error("load settings on connect failed", "device_id", deviceID, "err", err)
	} else if cur != nil {
		sess.PushSettings(settingsToProto(cur))
	}

	// Both loops report into buffered channels, and the handler returns as soon
	// as any one of them is done.
	//
	// Waiting for *both* would hang: cancelling this context does not unblock a
	// goroutine parked in stream.Recv(), and only returning from the handler
	// makes gRPC tear the stream down. So a replaced session would otherwise
	// keep its handler (and the client) waiting forever. The buffered channels
	// let the orphaned loop finish and exit on its own.
	recvErr := make(chan error, 1)
	sendErr := make(chan error, 1)
	go func() { recvErr <- s.sessionRecv(ctx, stream, sess) }()
	go func() { sendErr <- s.sessionSend(ctx, stream, sess) }()

	select {
	case err = <-recvErr:
	case err = <-sendErr:
	case <-ctx.Done():
		err = context.Cause(ctx)
	}
	s.log.Info("device disconnected", "device_id", deviceID, "err", err)

	// A replaced session or a clean client hang-up is a normal ending.
	if errors.Is(context.Cause(ctx), session.ErrReplaced) {
		return status.Error(codes.Aborted, "replaced by a newer connection from this device")
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// sessionRecv owns reading from the stream.
func (s *Server) sessionRecv(ctx context.Context, stream skycamv1.SkycamService_DeviceSessionServer, sess *session.Session) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil // device closed its side
		}
		if err != nil {
			return err
		}
		sess.MarkSeen()

		switch m := msg.Msg.(type) {
		case *skycamv1.DeviceSessionRequest_Telemetry:
			reading := store.TelemetryReading{
				DeviceID:     sess.DeviceID,
				RecordedAt:   m.Telemetry.GetRecordedAt().AsTime(),
				TemperatureC: m.Telemetry.TemperatureC,
				HumidityPct:  m.Telemetry.HumidityPct,
				ProbeTempC:   m.Telemetry.ProbeTempC,
			}
			if reading.RecordedAt.IsZero() {
				reading.RecordedAt = time.Now().UTC()
			}
			if err := s.telemetry.InsertTelemetry(ctx, reading); err != nil {
				// Losing one reading is not worth dropping the session: the
				// next one is seconds away, and the session also carries
				// commands that matter more.
				s.log.Error("telemetry insert failed", "device_id", sess.DeviceID, "err", err)
			}
		case *skycamv1.DeviceSessionRequest_Heartbeat:
			// MarkSeen above is the point; nothing else to do.
		case *skycamv1.DeviceSessionRequest_CommandAck:
			sess.DeliverAck(m.CommandAck)
		case *skycamv1.DeviceSessionRequest_Hello:
			return status.Error(codes.InvalidArgument, "Hello may only be the first message")
		default:
			return status.Error(codes.InvalidArgument, "unknown message on DeviceSession")
		}
	}
}

// sessionSend owns writing to the stream, and is the only goroutine that may.
// It also enforces the idle deadline, so a peer that stops talking is dropped
// even if the socket still looks open to us.
func (s *Server) sessionSend(ctx context.Context, stream skycamv1.SkycamService_DeviceSessionServer, sess *session.Session) error {
	idle := s.limits.SessionIdleTimeout
	ticker := time.NewTicker(idle / 3)
	defer ticker.Stop()

	sendCtx, stop := context.WithCancel(ctx)
	defer stop()

	out := make(chan *skycamv1.DeviceSessionResponse)
	errc := make(chan error, 1)
	// Next blocks, so it runs here and hands messages back; this goroutine ends
	// when sendCtx is cancelled by the defer above.
	go func() {
		defer close(out)
		for {
			msg, err := sess.Next(sendCtx)
			if err != nil {
				errc <- err
				return
			}
			select {
			case out <- msg:
			case <-sendCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errc:
			return err
		case msg := <-out:
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-ticker.C:
			if time.Since(sess.LastSeen()) > idle {
				s.log.Info("session idle timeout", "device_id", sess.DeviceID, "idle", idle)
				return status.Error(codes.DeadlineExceeded, "no messages from device within the idle timeout")
			}
			if err := s.devices.TouchDevice(ctx, sess.DeviceID, sess.LastSeen()); err != nil {
				s.log.Error("touch device failed", "device_id", sess.DeviceID, "err", err)
			}
		}
	}
}

// forwardBusCommands pushes commands routed from other replicas onto this
// device's local session queue, and publishes the device's ack back to whichever
// replica is waiting.
//
// It reuses the ordinary session queue rather than touching the stream, so a
// remote command is subject to exactly the same bounded queue and single-writer
// rules as a local one. Nothing about the device's side of the connection knows
// or cares that the command arrived over Redis.
func (s *Server) forwardBusCommands(ctx context.Context, deviceID string, sess *session.Session) error {
	cmds, err := s.bus.Commands(ctx, deviceID)
	if err != nil {
		return err
	}
	go func() {
		for cmd := range cmds {
			proto := &skycamv1.Command{CommandId: cmd.CommandID}
			switch cmd.Kind {
			case "capture_now":
				proto.Kind = &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}
			case "abort_exposure":
				proto.Kind = &skycamv1.Command_AbortExposure{AbortExposure: &skycamv1.AbortExposure{}}
			default:
				s.log.Warn("unknown command kind on the bus", "kind", cmd.Kind)
				continue
			}

			// Waiting for the device's ack here, then relaying it, keeps the
			// waiting operator on the other replica blocked on one thing rather
			// than correlating two hops itself.
			ackCtx, cancel := context.WithTimeout(ctx, busAckTimeout)
			ack, err := sess.SendCommand(ackCtx, proto, true)
			cancel()

			out := cluster.Ack{CommandID: cmd.CommandID}
			switch {
			case err != nil:
				out.Error = err.Error()
			case ack == nil:
				out.Error = "sent, but no acknowledgement from the device"
			default:
				out.OK = ack.GetOk()
				out.Error = ack.GetError()
			}
			if err := s.bus.SendAck(ctx, cmd.ReplyTo, out); err != nil {
				s.log.Warn("failed to return ack over the bus",
					"command_id", cmd.CommandID, "err", err)
			}
		}
	}()
	return nil
}

func settingsToProto(s *store.Settings) *skycamv1.DeviceSettings {
	return &skycamv1.DeviceSettings{
		DeviceId:          s.DeviceID,
		ExposureMs:        s.ExposureMs,
		Gain:              s.Gain,
		PreviewGamma:      s.PreviewGamma,
		PreviewContrast:   s.PreviewContrast,
		PreviewBrightness: s.PreviewBrightness,
		UpdatedAt:         timestamppb.New(s.UpdatedAt),
	}
}
