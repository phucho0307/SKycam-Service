package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisBus implements Bus over one Redis instance.
type RedisBus struct {
	rdb       *redis.Client
	cfg       Config
	replicaID string
	log       *slog.Logger

	// One subscription for this replica's acks; a subscription per in-flight
	// command would open a Redis connection per command. Demultiplexed to
	// waiters by command id.
	ackOnce sync.Once
	ackErr  error
	mu      sync.Mutex
	waiters map[string]chan Ack
}

func NewRedisBus(rdb *redis.Client, cfg Config, log *slog.Logger) *RedisBus {
	return &RedisBus{
		rdb: rdb, cfg: cfg.withDefaults(),
		// Random per process. A pod name would be prettier but must not be
		// *reused*: a restarted pod with the same id could receive acks meant for
		// its predecessor's in-flight commands.
		replicaID: uuid.NewString(),
		waiters:   make(map[string]chan Ack),
		log:       log,
	}
}

func (b *RedisBus) ReplicaID() string { return b.replicaID }
func (b *RedisBus) Close() error      { return nil } // the client is owned by the caller

func (b *RedisBus) sessionKey(deviceID string) string {
	return fmt.Sprintf("%s:session:%s", b.cfg.KeyPrefix, deviceID)
}
func (b *RedisBus) cmdChannel(deviceID string) string {
	return fmt.Sprintf("%s:cmd:%s", b.cfg.KeyPrefix, deviceID)
}
func (b *RedisBus) ackChannel(replica string) string {
	return fmt.Sprintf("%s:ack:%s", b.cfg.KeyPrefix, replica)
}

// Announce claims the device for this replica and keeps the claim fresh.
func (b *RedisBus) Announce(ctx context.Context, deviceID string) (func(), error) {
	key := b.sessionKey(deviceID)
	// Plain SET, not SETNX: a reconnecting device legitimately replaces its own
	// older session, and the server's own registry already decides which stream
	// wins. Refusing here would strand a camera behind its own stale key until
	// the TTL expired.
	if err := b.rdb.Set(ctx, key, b.replicaID, b.cfg.TTL).Err(); err != nil {
		return nil, fmt.Errorf("announce %s: %w", deviceID, err)
	}

	renewCtx, stop := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(b.cfg.RenewEvery)
		defer t.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-t.C:
				// Renewed unconditionally rather than only if still ours: if
				// another replica took over, its own renewal wins the race and
				// this session is about to be torn down anyway.
				if err := b.rdb.Set(renewCtx, key, b.replicaID, b.cfg.TTL).Err(); err != nil &&
					!errors.Is(err, context.Canceled) {
					b.log.Warn("presence renewal failed", "device_id", deviceID, "err", err)
				}
			}
		}
	}()

	return func() {
		stop()
		// Delete only if it is still ours. Without the compare, a slow teardown
		// would evict the claim of a *newer* session for the same device -- the
		// same identity check session.Registry.Remove already makes locally.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		const delIfMine = `if redis.call("GET", KEYS[1]) == ARGV[1] then
		                       return redis.call("DEL", KEYS[1])
		                   end
		                   return 0`
		if err := b.rdb.Eval(releaseCtx, delIfMine, []string{key}, b.replicaID).Err(); err != nil &&
			!errors.Is(err, redis.Nil) {
			b.log.Warn("presence release failed", "device_id", deviceID, "err", err)
		}
	}, nil
}

func (b *RedisBus) Holder(ctx context.Context, deviceID string) (string, error) {
	v, err := b.rdb.Get(ctx, b.sessionKey(deviceID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrNotConnected
	}
	if err != nil {
		return "", fmt.Errorf("lookup holder: %w", err)
	}
	return v, nil
}

// Connected scans presence keys.
//
// SCAN rather than KEYS: KEYS blocks Redis's single command loop for the whole
// keyspace, which at any real size stalls every other client including the ones
// routing commands.
func (b *RedisBus) Connected(ctx context.Context) ([]Presence, error) {
	prefix := fmt.Sprintf("%s:session:", b.cfg.KeyPrefix)
	var out []Presence
	iter := b.rdb.Scan(ctx, 0, prefix+"*", 200).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan sessions: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := b.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("read sessions: %w", err)
	}
	for i, v := range vals {
		// A key can expire between SCAN and MGET; a nil is simply a device that
		// has just gone away.
		if v == nil {
			continue
		}
		replica, _ := v.(string)
		out = append(out, Presence{
			DeviceID: strings.TrimPrefix(keys[i], prefix),
			Replica:  replica,
		})
	}
	return out, nil
}

// Deliver publishes a command to the holder's channel.
func (b *RedisBus) Deliver(ctx context.Context, cmd Command) error {
	// Checked first so an offline device fails immediately instead of waiting out
	// the ack timeout: pub/sub cannot report whether anyone was subscribed.
	if _, err := b.Holder(ctx, cmd.DeviceID); err != nil {
		return err
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	n, err := b.rdb.Publish(ctx, b.cmdChannel(cmd.DeviceID), payload).Result()
	if err != nil {
		return fmt.Errorf("publish command: %w", err)
	}
	if n == 0 {
		// The key said someone holds it but nobody is subscribed: the holder died
		// between its last renewal and now. Report it as disconnected rather than
		// letting the caller wait for an ack that cannot come.
		return ErrNotConnected
	}
	return nil
}

func (b *RedisBus) Commands(ctx context.Context, deviceID string) (<-chan Command, error) {
	sub := b.rdb.Subscribe(ctx, b.cmdChannel(deviceID))
	// Wait for the subscription to be live before returning. Otherwise a command
	// published immediately after Announce could land before SUBSCRIBE completed
	// and be silently dropped.
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("subscribe commands: %w", err)
	}

	out := make(chan Command, 8)
	go func() {
		defer close(out)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var cmd Command
				if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
					b.log.Warn("undecodable command on the bus", "err", err)
					continue
				}
				select {
				case out <- cmd:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (b *RedisBus) SendAck(ctx context.Context, replyTo string, ack Ack) error {
	payload, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return b.rdb.Publish(ctx, b.ackChannel(replyTo), payload).Err()
}

// AwaitAck registers a waiter for one command id.
//
// One subscription per process, demultiplexed by command id -- the same shape
// session.Session uses for device acks, for the same reason: a shared channel
// with several consumers means each one steals and drops the others' messages.
func (b *RedisBus) AwaitAck(commandID string) (<-chan Ack, func(), error) {
	if err := b.ensureAckSub(); err != nil {
		return nil, func() {}, err
	}
	ch := make(chan Ack, 1)
	b.mu.Lock()
	b.waiters[commandID] = ch
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		delete(b.waiters, commandID)
		b.mu.Unlock()
	}, nil
}

// ensureAckSub starts this replica's single ack subscription.
func (b *RedisBus) ensureAckSub() error {
	b.ackOnce.Do(func() {
		// context.Background() deliberately: this subscription outlives the
		// request that first needed it. Tying it to that request would tear it
		// down as soon as one command finished.
		sub := b.rdb.Subscribe(context.Background(), b.ackChannel(b.replicaID))
		if _, err := sub.Receive(context.Background()); err != nil {
			b.ackErr = fmt.Errorf("subscribe acks: %w", err)
			return
		}
		go func() {
			for msg := range sub.Channel() {
				var ack Ack
				if err := json.Unmarshal([]byte(msg.Payload), &ack); err != nil {
					continue
				}
				b.mu.Lock()
				ch, ok := b.waiters[ack.CommandID]
				b.mu.Unlock()
				if !ok {
					// The waiter timed out and went away. Dropping is correct;
					// there is nobody left to care.
					b.log.Debug("ack with no waiter", "command_id", ack.CommandID)
					continue
				}
				select {
				case ch <- ack:
				default:
					// Capacity 1 and already filled: a duplicate ack.
				}
			}
		}()
	})
	return b.ackErr
}
