package authsvc

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/oidc"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("INGEST_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/13"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("no Redis at %s: %v", url, err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// The whole browser flow -- login, provider, callback -- across two replicas
// behind a round-robin balancer, so the login and its callback are guaranteed
// to hit different instances.
func TestSignInAcrossReplicasWithSharedStore(t *testing.T) {
	rdb := testRedis(t)
	p := "t-" + uuid.NewString()[:8]
	r := newReplicatedRig(t, 2, func(c *Config) {
		// Each replica gets its own RedisFlows value, as separate pods would;
		// only the Redis behind them is shared.
		c.Flows = oidc.NewRedisFlows(rdb, time.Minute, p)
	})
	r.users.users["google-oauth2|1234567890"] = store.User{UserID: "google-oauth2|1234567890"}

	for i := range 5 {
		resp := r.signIn(t)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("sign-in %d across replicas: status %d, want 200", i, resp.StatusCode)
		}
	}
}

// Control: the same two replicas with the in-memory default fail. Without this,
// the test above would also pass for a balancer that never actually alternated.
func TestSignInAcrossReplicasFailsWithInMemoryStore(t *testing.T) {
	r := newReplicatedRig(t, 2, nil)
	r.users.users["google-oauth2|1234567890"] = store.User{UserID: "google-oauth2|1234567890"}

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("in-memory flows across replicas gave %d; expected 400 (unknown state)", resp.StatusCode)
	}
}
