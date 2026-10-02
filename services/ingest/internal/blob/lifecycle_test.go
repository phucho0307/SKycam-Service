package blob_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/blob"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/config"
)

// The bucket's AbortIncompleteMultipartUpload lifecycle rule deletes upload
// parts left open longer than its window (infra/k8s/minio/bucket-init.yaml).
// A camera offline for longer than that comes back to find its half-finished
// FITS gone. It must start over cleanly, not fail forever on an upload id that
// no longer exists.
//
// Simulated by aborting the upload directly, which is exactly what the rule
// does; waiting a real day for MinIO's scanner is not a test.
func TestResumeAfterLifecycleAbortedTheUpload(t *testing.T) {
	endpoint := os.Getenv("INGEST_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set INGEST_TEST_S3_ENDPOINT to run S3 integration tests")
	}
	ctx := context.Background()
	cfg := config.S3{
		Endpoint: endpoint, Region: "us-east-1",
		Bucket:    envOr("INGEST_TEST_S3_BUCKET", "ingest-test"),
		AccessKey: envOr("INGEST_TEST_S3_ACCESS_KEY", "minioadmin"),
		SecretKey: envOr("INGEST_TEST_S3_SECRET_KEY", "minioadmin"),
	}
	store, err := blob.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	raw := rawClient(t, cfg)

	key := "frames/lifecycle-test/" + uuid.NewString() + ".fits"
	file := make([]byte, 2*blob.PartSize+1234) // three parts
	rand.Read(file)
	t.Cleanup(func() { _ = store.Delete(ctx, key) })

	// The camera uploads one part, then goes offline.
	up, err := store.Resume(ctx, key, "application/fits", int64(len(file)))
	if err != nil {
		t.Fatal(err)
	}
	if err := up.WritePart(ctx, file[:blob.PartSize]); err != nil {
		t.Fatal(err)
	}
	if p, _ := store.Progress(ctx, key); p.Committed != blob.PartSize {
		t.Fatalf("before abort: committed %d, want %d", p.Committed, blob.PartSize)
	}

	// The lifecycle rule fires while it is away.
	n := abortAll(t, raw, cfg.Bucket, key)
	if n != 1 {
		t.Fatalf("expected to abort exactly 1 open upload, aborted %d", n)
	}

	// It comes back: progress reads as nothing stored, not as an error...
	p, err := store.Progress(ctx, key)
	if err != nil {
		t.Fatalf("progress after abort should be zero, not an error: %v", err)
	}
	if p.Committed != 0 {
		t.Fatalf("progress after abort: committed %d, want 0", p.Committed)
	}

	// ...and resuming starts a fresh upload that completes and verifies.
	up, err = store.Resume(ctx, key, "application/fits", int64(len(file)))
	if err != nil {
		t.Fatalf("resume after abort: %v", err)
	}
	if up.Committed() != 0 {
		t.Fatalf("fresh upload should start at 0, starts at %d", up.Committed())
	}
	for off := 0; off < len(file); off += blob.PartSize {
		end := min(off+blob.PartSize, len(file))
		if err := up.WritePart(ctx, file[off:end]); err != nil {
			t.Fatalf("write part at %d: %v", off, err)
		}
	}
	if err := up.Complete(ctx); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: &cfg.Bucket, Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(got.Body)
	if !bytes.Equal(buf.Bytes(), file) {
		t.Fatal("object assembled after the abort does not match the file sent")
	}
}

func abortAll(t *testing.T, c *s3.Client, bucket, key string) int {
	t.Helper()
	ctx := context.Background()
	out, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket, Prefix: &key})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, u := range out.Uploads {
		if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: &bucket, Key: u.Key, UploadId: u.UploadId,
		}); err != nil {
			t.Fatal(err)
		}
		n++
	}
	return n
}

func rawClient(t *testing.T, cfg config.S3) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	if err != nil {
		t.Fatal(err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
