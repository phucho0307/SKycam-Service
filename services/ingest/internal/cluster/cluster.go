// Package cluster lets several ingest replicas reach a device whose gRPC stream
// is held by one of them.
//
// The problem it exists for: `session.Registry` is a per-process map, so with two
// replicas an operator calling SendCommand on replica A cannot reach a camera
// whose DeviceSession lives on replica B -- the `chan *Command` is in B's memory.
// That is a correctness failure, not a performance one, and it caps the service
// at one replica. One replica is not a throughput ceiling; it is an availability
// ceiling, because every deploy then disconnects every camera.
//
// Two Redis features, doing two different jobs:
//
//   - an expiring key `session:<device> = <replica>` answers *where* a device is.
//     Pub/sub cannot tell you whether anyone was listening, so without this a
//     command to an offline camera would be published into the void and only fail
//     after the ack timeout. The TTL is what makes a crashed replica's claim
//     expire on its own -- Postgres has none, so it would need a sweeper.
//   - pub/sub carries the command to that replica, and the ack back.
//
// **Why lossy pub/sub is acceptable here and nowhere else in this system.** The
// contract already says commands are not stored, because "abort the exposure" is
// meaningless an hour later, and SendCommand already reports *sent but
// unacknowledged* as distinct from failed. A dropped publish degrades into a path
// that already exists and is already handled. Frames, telemetry, detection
// triggers and rain alerts all get durable rows instead.
package cluster

import (
	"context"
	"errors"
	"time"
)

// ErrNotConnected means no replica currently holds a session for that device.
var ErrNotConnected = errors.New("device is not connected to any replica")

// Command is the payload routed between replicas. Deliberately not the protobuf
// type: the bus should not care what it is carrying, and a narrow struct keeps
// the wire format explicit rather than incidental.
type Command struct {
	DeviceID  string `json:"device_id"`
	CommandID string `json:"command_id"`
	// Kind is "capture_now" or "abort_exposure".
	Kind string `json:"kind"`
	// ReplyTo identifies the replica waiting for the ack, so the holder knows
	// where to send it. Carried explicitly rather than inferred, so an ack is
	// never broadcast to every replica.
	ReplyTo string `json:"reply_to"`
}

// Ack travels back the other way.
type Ack struct {
	CommandID string `json:"command_id"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

// Presence is what the registry knows about one connected device.
type Presence struct {
	DeviceID string
	Replica  string
}

// Bus is the cross-replica transport. An interface so tests can run several
// "replicas" without Redis, and so Redis is not welded into the server.
type Bus interface {
	// Announce records that this replica holds a session for deviceID, and keeps
	// renewing it until ctx ends. Returns a function that removes the claim.
	Announce(ctx context.Context, deviceID string) (release func(), err error)
	// Holder reports which replica holds the device, or ErrNotConnected.
	Holder(ctx context.Context, deviceID string) (string, error)
	// Connected lists every device any replica holds.
	Connected(ctx context.Context) ([]Presence, error)
	// Deliver routes a command to the replica holding the device.
	Deliver(ctx context.Context, cmd Command) error
	// Commands yields commands addressed to devices this replica holds. The
	// returned channel closes when ctx ends.
	Commands(ctx context.Context, deviceID string) (<-chan Command, error)
	// SendAck returns an acknowledgement to the replica named in cmd.ReplyTo.
	SendAck(ctx context.Context, replyTo string, ack Ack) error
	// AwaitAck registers interest in one command's acknowledgement and returns a
	// channel that receives it, plus a cleanup to call when done.
	//
	// Correlation is the bus's job, not the caller's. Handing every waiter the
	// same channel looked simpler and was wrong: with several commands in flight,
	// a waiter reads an ack for somebody else's command, discards it, and the
	// rightful waiter times out. Register BEFORE publishing, or a fast reply can
	// arrive before anyone is listening.
	AwaitAck(commandID string) (acks <-chan Ack, cancel func(), err error)
	// ReplicaID is this process's identity on the bus.
	ReplicaID() string
	Close() error
}

// Config sizes the presence keys.
type Config struct {
	// TTL on a presence key. Must exceed RenewEvery by a comfortable margin, or a
	// slow renewal makes a live device look disconnected.
	TTL time.Duration
	// RenewEvery is how often a held session refreshes its key.
	RenewEvery time.Duration
	// KeyPrefix namespaces everything, so one Redis can serve dev and test.
	KeyPrefix string
}

func (c Config) withDefaults() Config {
	if c.TTL <= 0 {
		c.TTL = 30 * time.Second
	}
	if c.RenewEvery <= 0 {
		// A third of the TTL: two consecutive renewals can fail before a live
		// session looks dead.
		c.RenewEvery = c.TTL / 3
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "skycam"
	}
	return c
}
