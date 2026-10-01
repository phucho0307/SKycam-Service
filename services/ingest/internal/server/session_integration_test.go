package server

import (
	"context"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
)

// connect opens a DeviceSession and sends the opening Hello.
func connect(t *testing.T, h *harness, client skycamv1.SkycamServiceClient) (skycamv1.SkycamService_DeviceSessionClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.DeviceSession(ctx)
	if err != nil {
		cancel()
		t.Fatalf("open session: %v", err)
	}
	err = stream.Send(&skycamv1.DeviceSessionRequest{
		Msg: &skycamv1.DeviceSessionRequest_Hello{
			Hello: &skycamv1.Hello{DeviceId: h.device, ClientVersion: "test"},
		},
	})
	if err != nil {
		cancel()
		t.Fatalf("send hello: %v", err)
	}
	waitUntil(t, 5*time.Second, func() bool { _, ok := h.srv.Sessions().Get(h.device); return ok })
	return stream, cancel
}

func gain(v int64) *int64 { return &v }

// The operator changes a setting while the camera is connected: it arrives on
// the open stream without the device asking for it.
func TestSettingsPushReachesConnectedDevice(t *testing.T) {
	h := newHarness(t)
	stream, cancel := connect(t, h, h.client)
	defer cancel()

	resp, err := h.control.UpdateDeviceSettings(context.Background(), &skycamv1.UpdateDeviceSettingsRequest{
		Settings: &skycamv1.DeviceSettings{DeviceId: h.device, Gain: gain(42)},
	})
	if err != nil {
		t.Fatalf("update settings: %v", err)
	}
	if !resp.GetDelivered() {
		t.Fatal("delivered=false while the device was connected")
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got := msg.GetSettings().GetGain(); got != 42 {
		t.Fatalf("device received gain %d, want 42", got)
	}
}

// The camera is offline: the change is still stored, and it is handed over on
// the next connect. This is what makes a lost push harmless.
func TestSettingsStoredWhileOfflineAndSentOnConnect(t *testing.T) {
	h := newHarness(t)

	resp, err := h.control.UpdateDeviceSettings(context.Background(), &skycamv1.UpdateDeviceSettingsRequest{
		Settings: &skycamv1.DeviceSettings{DeviceId: h.device, Gain: gain(7)},
	})
	if err != nil {
		t.Fatalf("update settings: %v", err)
	}
	if resp.GetDelivered() {
		t.Fatal("delivered=true with no device connected")
	}

	stream, cancel := connect(t, h, h.client)
	defer cancel()
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got := msg.GetSettings().GetGain(); got != 7 {
		t.Fatalf("on connect the device received gain %d, want 7", got)
	}
}

// Every change is recorded with what it replaced.
func TestSettingsChangesAreAudited(t *testing.T) {
	h := newHarness(t)
	for _, g := range []int64{10, 20} {
		if _, err := h.control.UpdateDeviceSettings(context.Background(), &skycamv1.UpdateDeviceSettingsRequest{
			Settings: &skycamv1.DeviceSettings{DeviceId: h.device, Gain: gain(g)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	var before, after *int64
	err := h.env.Pool.QueryRow(context.Background(), `
		SELECT (before_json->>'gain')::bigint, (after_json->>'gain')::bigint
		FROM settings_audit WHERE device_id = $1 ORDER BY changed_at DESC LIMIT 1`,
		h.device).Scan(&before, &after)
	if err != nil {
		t.Fatalf("audit row: %v", err)
	}
	if before == nil || *before != 10 || after == nil || *after != 20 {
		t.Fatalf("audit recorded before=%v after=%v, want 10 -> 20", before, after)
	}
}

func TestTelemetryOnSessionIsStored(t *testing.T) {
	h := newHarness(t)
	stream, cancel := connect(t, h, h.client)
	defer cancel()

	recordedAt := time.Now().UTC().Truncate(time.Millisecond)
	temp := 4.5
	err := stream.Send(&skycamv1.DeviceSessionRequest{
		Msg: &skycamv1.DeviceSessionRequest_Telemetry{Telemetry: &skycamv1.Telemetry{
			RecordedAt: timestamppb.New(recordedAt), TemperatureC: &temp,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got float64
	waitUntil(t, 5*time.Second, func() bool {
		return h.env.Pool.QueryRow(context.Background(),
			`SELECT temperature_c FROM telemetry WHERE device_id = $1 AND recorded_at = $2`,
			h.device, recordedAt).Scan(&got) == nil
	})
	if got != temp {
		t.Fatalf("stored temperature %v, want %v", got, temp)
	}
}

// A camera that reconnects after a half-open socket must win; the stale session
// is closed rather than competing with it.
func TestReconnectReplacesTheOlderSession(t *testing.T) {
	h := newHarness(t)
	first, cancelFirst := connect(t, h, h.client)
	defer cancelFirst()

	second, cancelSecond := connect(t, h, h.client)
	defer cancelSecond()

	// The displaced stream ends with Aborted.
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("old session ended with %v, want Aborted", err)
	}
	if h.srv.Sessions().Count() != 1 {
		t.Fatalf("registry holds %d sessions, want 1", h.srv.Sessions().Count())
	}
	// The new session is the live one and still receives pushes.
	if _, err := h.control.UpdateDeviceSettings(context.Background(), &skycamv1.UpdateDeviceSettingsRequest{
		Settings: &skycamv1.DeviceSettings{DeviceId: h.device, Gain: gain(99)},
	}); err != nil {
		t.Fatal(err)
	}
	msg, err := second.Recv()
	if err != nil || msg.GetSettings().GetGain() != 99 {
		t.Fatalf("new session got %v, err %v", msg, err)
	}
}

// A device that goes quiet — including heartbeats — is dropped, even though the
// socket still looks open from this side.
func TestIdleSessionIsDropped(t *testing.T) {
	h := newHarnessWithIdle(t, 900*time.Millisecond)
	stream, cancel := connect(t, h, h.client)
	defer cancel()

	_, err := stream.Recv() // never sends anything after Hello
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("idle session ended with %v, want DeadlineExceeded", err)
	}
	waitUntil(t, 5*time.Second, func() bool { return h.srv.Sessions().Count() == 0 })
}

// Sessions are long-lived, so a leak here accumulates one goroutine per
// reconnect until the process dies.
func TestSessionsDoNotLeakGoroutines(t *testing.T) {
	h := newHarness(t)

	// One cycle first, so lazily-started gRPC machinery isn't counted.
	_, cancel := connect(t, h, h.client)
	cancel()
	waitUntil(t, 5*time.Second, func() bool { return h.srv.Sessions().Count() == 0 })
	time.Sleep(200 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		_, cancel := connect(t, h, h.client)
		cancel()
		waitUntil(t, 5*time.Second, func() bool { return h.srv.Sessions().Count() == 0 })
	}

	var final int
	waitUntil(t, 5*time.Second, func() bool {
		final = runtime.NumGoroutine()
		return final <= baseline+2
	})
	if final > baseline+2 {
		t.Fatalf("goroutines grew from %d to %d over 5 connect/disconnect cycles", baseline, final)
	}
}

func TestCommandToDisconnectedDeviceFails(t *testing.T) {
	h := newHarness(t)
	_, err := h.control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId: h.device,
		Command:  &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition (commands are not queued for later)", err)
	}
}

// The device answers a command on the same stream, and the ack finds its way
// back to the waiting operator call.
func TestCommandRoundTripWithAck(t *testing.T) {
	h := newHarness(t)
	stream, cancel := connect(t, h, h.client)
	defer cancel()

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			if cmd := msg.GetCommand(); cmd != nil {
				_ = stream.Send(&skycamv1.DeviceSessionRequest{
					Msg: &skycamv1.DeviceSessionRequest_CommandAck{
						CommandAck: &skycamv1.CommandAck{CommandId: cmd.GetCommandId(), Ok: true},
					},
				})
			}
		}
	}()

	resp, err := h.control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId:     h.device,
		Command:      &skycamv1.Command{Kind: &skycamv1.Command_AbortExposure{AbortExposure: &skycamv1.AbortExposure{}}},
		AckTimeoutMs: 5000,
	})
	if err != nil || !resp.GetAcknowledged() {
		t.Fatalf("acknowledged=%v err=%v", resp.GetAcknowledged(), err)
	}
}

func TestListConnectedDevices(t *testing.T) {
	h := newHarness(t)
	_, cancel := connect(t, h, h.client)
	defer cancel()

	resp, err := h.control.ListConnectedDevices(context.Background(), &skycamv1.ListConnectedDevicesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetDevices()) != 1 || resp.GetDevices()[0].GetDeviceId() != h.device {
		t.Fatalf("got %v, want exactly %s", resp.GetDevices(), h.device)
	}
}

// ---- auth ----

func TestUnknownTokenIsRejected(t *testing.T) {
	h := newHarness(t)
	bad := h.dial(t, "not-a-real-token")
	_, err := bad.GetUploadStatus(context.Background(),
		&skycamv1.GetUploadStatusRequest{FrameId: newFrame(0).id.String()})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v, want Unauthenticated", err)
	}
}

// Without this, anyone who can reach the port could open a session claiming to
// be a camera and evict the real one.
func TestSessionRequiresAuthentication(t *testing.T) {
	h := newHarness(t)
	bad := h.dial(t, "not-a-real-token")
	stream, err := bad.DeviceSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&skycamv1.DeviceSessionRequest{
		Msg: &skycamv1.DeviceSessionRequest_Hello{Hello: &skycamv1.Hello{DeviceId: h.device}},
	})
	if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v, want Unauthenticated", err)
	}
	if h.srv.Sessions().Count() != 0 {
		t.Fatal("an unauthenticated caller registered a session")
	}
}

// A stolen device token must not let one camera write as another.
func TestCannotActAsAnotherDevice(t *testing.T) {
	h := newHarness(t)
	f := newFrame(0)
	hdr := f.header(h.device)
	hdr.DeviceId = "some-other-camera"

	_, err := h.send(t, f, hdr, 0, 0, true)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("got %v, want PermissionDenied", err)
	}
}

// A device token is not an operator token: cameras cannot change their own
// settings or command their neighbours.
func TestDeviceTokenCannotUseControlAPI(t *testing.T) {
	h := newHarness(t)
	ctrl := skycamv1.NewSkycamControlServiceClient(h.dialConn(t, h.token))
	_, err := ctrl.UpdateDeviceSettings(context.Background(), &skycamv1.UpdateDeviceSettingsRequest{
		Settings: &skycamv1.DeviceSettings{DeviceId: h.device, Gain: gain(1)},
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v, want Unauthenticated", err)
	}
}
