package server

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/cluster"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/session"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

// ControlServer is the operator-facing API. It is a separate gRPC service so it
// can listen on an internal-only port: anyone able to reach it can re-point a
// live camera.
type ControlServer struct {
	skycamv1.UnimplementedSkycamControlServiceServer
	*Server
}

// authorize answers "may this caller do `required` to this device?".
//
// The interceptor established *who* the caller is; this decides what they may
// do, and it is deliberately here rather than in the interceptor because the
// answer depends on the request body (which device). The caller's identity
// comes from the verified token and never from the request — the same rule the
// device API applies, for the same reason.
//
// Returns the user id to record in the audit trail.
func (c *ControlServer) authorize(ctx context.Context, deviceID string, required store.Role) (string, error) {
	p, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "authentication required")
	}
	if p.ServiceAccount {
		// The shared operator token predates user auth and has no grants to
		// check. It is loopback-only and is being retired in favour of JWTs.
		return "operator", nil
	}

	access, err := c.grants.AccessFor(ctx, p.UserID, deviceID)
	switch {
	case errors.Is(err, store.ErrUnknownUser), errors.Is(err, store.ErrUserDisabled):
		// A validly signed token for someone we do not know, or no longer
		// allow. Authentication succeeded; authorization did not.
		return "", status.Error(codes.PermissionDenied, "not authorized for this device")
	case err != nil:
		return "", c.unavailable("check authorization", err)
	}
	if !access.Allows(required) {
		// Same message whether the device does not exist, the user has no grant,
		// or the grant is too weak: otherwise this endpoint enumerates which
		// device ids are real.
		return "", status.Error(codes.PermissionDenied, "not authorized for this device")
	}
	return p.UserID, nil
}

// visibleDevices returns the devices this caller may see. The second result is
// true when the caller sees everything (platform admin or service account), in
// which case the map is nil rather than a copy of every device id.
func (c *ControlServer) visibleDevices(ctx context.Context) (map[string]store.Role, bool, error) {
	p, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return nil, false, status.Error(codes.Unauthenticated, "authentication required")
	}
	if p.ServiceAccount {
		return nil, true, nil
	}
	devices, all, err := c.grants.DevicesFor(ctx, p.UserID)
	switch {
	case errors.Is(err, store.ErrUnknownUser), errors.Is(err, store.ErrUserDisabled):
		// An empty list, not an error: nothing is visible to them.
		return map[string]store.Role{}, false, nil
	case err != nil:
		return nil, false, c.unavailable("list authorized devices", err)
	}
	return devices, all, nil
}

// UpdateDeviceSettings stores the change, then pushes it to the device if it
// happens to be connected.
//
// Storing first is the whole design: `delivered=false` is not a failure, it
// means the camera is offline and will pick the settings up when it reconnects.
func (c *ControlServer) UpdateDeviceSettings(ctx context.Context, req *skycamv1.UpdateDeviceSettingsRequest) (*skycamv1.UpdateDeviceSettingsResponse, error) {
	in := req.GetSettings()
	if in == nil || !deviceIDPattern.MatchString(in.GetDeviceId()) {
		return nil, status.Error(codes.InvalidArgument, "settings.device_id must match [A-Za-z0-9_-]{1,64}")
	}
	// Authorize before validating the payload. Validation first would answer
	// "gain out of range" to a caller with no access to this camera at all,
	// which is both a small information leak and the wrong order of concerns:
	// decide whether they may act, then whether the action is well-formed.
	// Changing exposure on a live camera is an operator action, not a viewer one.
	actor, err := c.authorize(ctx, in.GetDeviceId(), store.RoleOperator)
	if err != nil {
		return nil, err
	}

	if g := in.Gain; g != nil && (*g < 0 || *g > 1000) {
		return nil, status.Error(codes.InvalidArgument, "gain out of range 0..1000")
	}
	if e := in.ExposureMs; e != nil && (*e <= 0 || *e > 600000) {
		return nil, status.Error(codes.InvalidArgument, "exposure_ms out of range 0..600000")
	}

	saved, err := c.settings.UpsertSettings(ctx, store.Settings{
		DeviceID:          in.GetDeviceId(),
		ExposureMs:        in.ExposureMs,
		Gain:              in.Gain,
		PreviewGamma:      in.PreviewGamma,
		PreviewContrast:   in.PreviewContrast,
		PreviewBrightness: in.PreviewBrightness,
	}, actor)
	if err != nil {
		return nil, c.unavailable("store settings", err)
	}

	out := settingsToProto(&saved)
	delivered := false
	if sess, ok := c.sessions.Get(in.GetDeviceId()); ok {
		sess.PushSettings(out)
		delivered = true
	}
	c.log.Info("settings updated", "device_id", in.GetDeviceId(), "pushed", delivered)
	return &skycamv1.UpdateDeviceSettingsResponse{Settings: out, Delivered: delivered}, nil
}

// SendCommand reaches a connected device. Commands are deliberately not stored:
// "abort the current exposure" is meaningless an hour later, so an offline
// device is an error rather than a queued surprise.
func (c *ControlServer) SendCommand(ctx context.Context, req *skycamv1.SendCommandRequest) (*skycamv1.SendCommandResponse, error) {
	cmd := req.GetCommand()
	if cmd == nil || cmd.Kind == nil {
		return nil, status.Error(codes.InvalidArgument, "command.kind is required")
	}
	// Authorize before touching the session registry: "device is not connected"
	// would otherwise tell an unauthorized caller which devices exist and which
	// are online.
	if _, err := c.authorize(ctx, req.GetDeviceId(), store.RoleOperator); err != nil {
		return nil, err
	}
	if cmd.GetCommandId() == "" {
		cmd.CommandId = uuid.NewString()
	}

	// With a bus, the presence key is the single authority on where a device is,
	// and the local registry is only a fast path for when it agrees.
	//
	// Consulting it even when a local session exists costs one Redis GET, and it
	// fixes a real bug: after a device moves to another replica, this one keeps a
	// stale session until its stream notices it died — up to the 90s idle timeout
	// on a network partition. Trusting the local registry in that window sends
	// commands into a dead stream while the device is connected and reachable
	// elsewhere. Found by the test that moves a device between replicas.
	if c.bus != nil {
		holder, err := c.bus.Holder(ctx, req.GetDeviceId())
		switch {
		case errors.Is(err, cluster.ErrNotConnected):
			return nil, status.Errorf(codes.FailedPrecondition,
				"device %q is not connected", req.GetDeviceId())
		case err != nil:
			// Redis is down. Fall back to the local registry: degrading to
			// "commands work for devices on this replica" beats refusing all.
			c.log.Warn("presence lookup failed; falling back to the local registry",
				"device_id", req.GetDeviceId(), "err", err)
		case holder != c.bus.ReplicaID():
			return c.sendCommandViaBus(ctx, req, cmd)
		}
	}

	sess, ok := c.sessions.Get(req.GetDeviceId())
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "device %q is not connected", req.GetDeviceId())
	}

	wait := req.GetAckTimeoutMs() > 0
	if wait {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.GetAckTimeoutMs())*time.Millisecond)
		defer cancel()
	}

	ack, err := sess.SendCommand(ctx, cmd, wait)
	switch {
	case errors.Is(err, session.ErrBusy):
		return nil, status.Error(codes.ResourceExhausted, "device command queue is full")
	case errors.Is(err, context.DeadlineExceeded):
		// Sent, but unacknowledged: say so rather than implying it failed.
		return &skycamv1.SendCommandResponse{CommandId: cmd.GetCommandId(), Acknowledged: false,
			Error: "sent, but no acknowledgement within the timeout"}, nil
	case err != nil:
		return nil, c.unavailable("send command", err)
	}

	resp := &skycamv1.SendCommandResponse{CommandId: cmd.GetCommandId(), Acknowledged: ack != nil}
	if ack != nil && !ack.GetOk() {
		resp.Error = ack.GetError()
	}
	return resp, nil
}

// sendCommandViaBus routes a command to the replica holding the device's stream
// and waits for that replica to relay the device's acknowledgement.
//
// The presence lookup inside Deliver is what lets an offline device fail
// immediately: pub/sub cannot report whether anyone was subscribed, so without it
// a command to a camera that is simply not connected would look identical to one
// that was delivered and never answered.
func (c *ControlServer) sendCommandViaBus(ctx context.Context, req *skycamv1.SendCommandRequest,
	cmd *skycamv1.Command) (*skycamv1.SendCommandResponse, error) {

	kind := ""
	switch cmd.Kind.(type) {
	case *skycamv1.Command_CaptureNow:
		kind = "capture_now"
	case *skycamv1.Command_AbortExposure:
		kind = "abort_exposure"
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported command kind")
	}

	// Registered *before* publishing. The other order is a race: a fast replica
	// could answer before this one was listening, and the ack would be lost to a
	// timeout that looks like a dead device. Registering per command id is what
	// keeps concurrent commands from stealing each other's answers.
	acks, stopWaiting, err := c.bus.AwaitAck(cmd.GetCommandId())
	if err != nil {
		return nil, c.unavailable("register for the acknowledgement", err)
	}
	defer stopWaiting()

	busCmd := cluster.Command{
		DeviceID:  req.GetDeviceId(),
		CommandID: cmd.GetCommandId(),
		Kind:      kind,
		ReplyTo:   c.bus.ReplicaID(),
	}
	if err := c.bus.Deliver(ctx, busCmd); err != nil {
		if errors.Is(err, cluster.ErrNotConnected) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"device %q is not connected", req.GetDeviceId())
		}
		return nil, c.unavailable("route command", err)
	}

	if req.GetAckTimeoutMs() == 0 {
		// Fire and forget: the caller did not ask to wait.
		return &skycamv1.SendCommandResponse{CommandId: cmd.GetCommandId()}, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(req.GetAckTimeoutMs())*time.Millisecond)
	defer cancel()
	select {
	case <-waitCtx.Done():
		// Delivered but unacknowledged is not a failure, and saying so is the
		// difference between "retry" and "go and look at the camera".
		return &skycamv1.SendCommandResponse{
			CommandId: cmd.GetCommandId(), Acknowledged: false,
			Error: "routed to the holding replica, but no acknowledgement within the timeout",
		}, nil
	case ack := <-acks:
		return &skycamv1.SendCommandResponse{
			CommandId: cmd.GetCommandId(), Acknowledged: ack.OK, Error: ack.Error,
		}, nil
	}
}

// ListConnectedDevices reports this server's live sessions. With more than one
// replica each holds only its own, so an aggregated view would need a shared
// registry.
func (c *ControlServer) ListConnectedDevices(ctx context.Context, _ *skycamv1.ListConnectedDevicesRequest) (*skycamv1.ListConnectedDevicesResponse, error) {
	visible, all, err := c.visibleDevices(ctx)
	if err != nil {
		return nil, err
	}

	sessions := c.sessions.List()
	out := make([]*skycamv1.ConnectedDevice, 0, len(sessions))
	for _, s := range sessions {
		// Filtered, not authorized-or-denied: a user with access to one camera
		// gets a list of one, rather than an error naming devices they cannot see.
		if _, ok := visible[s.DeviceID]; !ok && !all {
			continue
		}
		out = append(out, &skycamv1.ConnectedDevice{
			DeviceId:      s.DeviceID,
			ConnectedAt:   timestamppb.New(s.ConnectedAt),
			LastMessageAt: timestamppb.New(s.LastSeen()),
		})
	}

	// Devices held by other replicas. Without this the list shows only this
	// pod's share -- a third of the fleet on three replicas, which reads as an
	// outage rather than as a partial view.
	//
	// Timestamps are omitted for remote devices: connected_at and last_message_at
	// live in the holding replica's memory, and inventing them here would be
	// worse than leaving them empty.
	if c.bus != nil {
		remote, err := c.bus.Connected(ctx)
		if err != nil {
			// A degraded list beats no list: local sessions are still accurate.
			c.log.Warn("could not list remote sessions", "err", err)
		} else {
			for _, p := range remote {
				if _, local := c.sessions.Get(p.DeviceID); local {
					continue
				}
				if _, ok := visible[p.DeviceID]; !ok && !all {
					continue
				}
				out = append(out, &skycamv1.ConnectedDevice{DeviceId: p.DeviceID})
			}
		}
	}
	return &skycamv1.ListConnectedDevicesResponse{Devices: out}, nil
}
