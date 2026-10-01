package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

// Load tests for the authorization path.
//
// They check two things at once, and the second matters more:
//
//  1. Throughput and latency hold up with many users hitting it at once.
//  2. **Authorization stays correct under concurrency.** A permission check
//     that is right when single-threaded and wrong under load is worse than one
//     that is obviously broken, so every user also attempts a write against a
//     camera they do not own, and every one of those must be refused.
//
// Skipped unless the integration environment is configured (see testenv).

// trespassGain is a valid setting that ONLY an unauthorized writer ever sends,
// so finding it on a camera proves a permission check failed. It must be inside
// the accepted range, or validation would reject the request before
// authorization ever runs -- which would make the test pass for the wrong reason.
const trespassGain = 900

type loadUser struct {
	id       string
	device   string // the one they may drive
	other    string // one they must not
	client   skycamv1.SkycamControlServiceClient
	conn     *grpc.ClientConn
	minted   string
	verified bool
}

func TestAuthorizationUnderLoad_100Users(t *testing.T) { runAuthzLoad(t, 100, 5) }

func TestAuthorizationUnderLoad_500Users(t *testing.T) {
	if testing.Short() {
		t.Skip("500-user run is slow; use -short to skip")
	}
	runAuthzLoad(t, 500, 5)
}

// runAuthzLoad provisions `users` users, each owning one camera, then has all of
// them issue `rounds` authorized and `rounds` unauthorized writes concurrently.
func runAuthzLoad(t *testing.T, userCount, rounds int) {
	h := newAuthzHarness(t)
	ctx := context.Background()

	// --- provision -----------------------------------------------------------
	setupStart := time.Now()
	devices := make([]string, userCount)
	for i := range devices {
		devices[i] = h.newDevice(t)
	}
	users := make([]*loadUser, userCount)
	for i := range users {
		id := h.newUser(t, false)
		// Each user may drive their own camera and no other. `other` is their
		// neighbour's, so a bug that ignores the device id shows up immediately.
		u := &loadUser{id: id, device: devices[i], other: devices[(i+1)%userCount]}
		h.grant(t, id, u.device, store.RoleOperator)
		u.minted = h.mint(t, id)
		u.conn = h.dial(t, u.minted)
		u.client = skycamv1.NewSkycamControlServiceClient(u.conn)
		users[i] = u
	}
	t.Logf("provisioned %d users + %d devices in %s", userCount, userCount, time.Since(setupStart).Round(time.Millisecond))

	// --- hammer it -----------------------------------------------------------
	var (
		allowed     atomic.Int64
		deniedOK    atomic.Int64
		wrongAllow  atomic.Int64 // a write that should have been refused
		wrongDeny   atomic.Int64 // a write that should have been allowed
		otherErrors atomic.Int64
	)
	var sampleErrMu sync.Mutex
	var sampleErr error
	noteErr := func(err error) {
		sampleErrMu.Lock()
		if sampleErr == nil {
			sampleErr = err
		}
		sampleErrMu.Unlock()
	}
	lat := make([][]time.Duration, userCount)

	start := time.Now()
	var wg sync.WaitGroup
	for i, u := range users {
		wg.Add(1)
		go func(i int, u *loadUser) {
			defer wg.Done()
			mine := make([]time.Duration, 0, rounds)
			for r := 0; r < rounds; r++ {
				// 1. The write they are entitled to make.
				t0 := time.Now()
				err := setGain(ctx, u.client, u.device, int64(100+r))
				mine = append(mine, time.Since(t0))
				switch {
				case err == nil:
					allowed.Add(1)
				case status.Code(err) == codes.PermissionDenied:
					wrongDeny.Add(1)
				default:
					otherErrors.Add(1)
					noteErr(err)
				}

				// 2. The write they must never be allowed to make.
				err = setGain(ctx, u.client, u.other, trespassGain)
				switch {
				case err == nil:
					wrongAllow.Add(1)
				case status.Code(err) == codes.PermissionDenied:
					deniedOK.Add(1)
				default:
					otherErrors.Add(1)
					noteErr(err)
				}
			}
			lat[i] = mine
		}(i, u)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// --- correctness first ---------------------------------------------------
	if n := wrongAllow.Load(); n != 0 {
		t.Fatalf("SECURITY: %d writes to another user's camera succeeded", n)
	}
	if n := wrongDeny.Load(); n != 0 {
		t.Fatalf("%d legitimate writes were refused", n)
	}
	if n := otherErrors.Load(); n != 0 {
		t.Fatalf("%d requests failed for reasons other than authorization; first: %v", n, sampleErr)
	}
	wantEach := int64(userCount * rounds)
	if allowed.Load() != wantEach || deniedOK.Load() != wantEach {
		t.Fatalf("allowed=%d denied=%d, want %d each", allowed.Load(), deniedOK.Load(), wantEach)
	}

	// Every user's own camera must hold *their* value, not 9999.
	for _, u := range users {
		got, err := h.pg.GetSettings(ctx, u.device)
		if err != nil {
			t.Fatalf("read back %s: %v", u.device, err)
		}
		if got == nil || got.Gain == nil {
			t.Fatalf("device %s has no settings", u.device)
		}
		if *got.Gain == trespassGain {
			t.Fatalf("SECURITY: device %s holds a value only an unauthorized writer used", u.device)
		}
	}

	// --- then the numbers ----------------------------------------------------
	var all []time.Duration
	for _, s := range lat {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	total := allowed.Load() + deniedOK.Load()
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		i := int(float64(len(all)-1) * p)
		return all[i]
	}
	t.Logf("%d users x %d rounds: %d authorized + %d denied = %d requests in %s (%.0f req/s)",
		userCount, rounds, allowed.Load(), deniedOK.Load(), total, elapsed.Round(time.Millisecond),
		float64(total)/elapsed.Seconds())
	t.Logf("authorized-write latency: p50=%s p95=%s p99=%s max=%s",
		pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond), all[len(all)-1].Round(time.Microsecond))
}

// BenchmarkAccessFor measures the authorization query alone -- the part that
// runs on every control-plane request and is the thing that would bite at scale.
func BenchmarkAccessFor(b *testing.B) {
	t := &testing.T{}
	h := newAuthzHarness(t)
	if h == nil {
		b.Skip("integration env not configured")
	}
	ctx := context.Background()

	// A realistic table, not a table of one row.
	const users, devicesPer = 200, 3
	var someUser, someDevice string
	for i := 0; i < users; i++ {
		u := h.newUser(t, false)
		for d := 0; d < devicesPer; d++ {
			dev := h.newDevice(t)
			h.grant(t, u, dev, store.RoleOperator)
			someUser, someDevice = u, dev
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := h.pg.AccessFor(ctx, someUser, someDevice); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	fmt.Fprintf(nopWriter{}, "%v", b.Elapsed())
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
