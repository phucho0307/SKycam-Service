// Package blob stores frame files in S3-compatible object storage.
//
// FITS uploads are resumable. S3 is the source of truth for progress: the
// object key is deterministic per frame, so after a dropped connection (or a
// server restart) the in-flight multipart upload and its completed parts are
// found again with ListMultipartUploads + ListParts. No separate state to keep
// consistent.
package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/config"
)

// ErrSizeConflict means stored parts or objects don't match the declared size,
// e.g. a client reused a frame_id for a different file.
var ErrSizeConflict = errors.New("stored data does not match declared size")

type Store struct {
	client *s3.Client
	bucket string
}

func New(ctx context.Context, cfg config.S3) (*Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true // MinIO / R2
		// Only send checksum headers when the API requires them; some
		// S3-compatible stores reject the SDK's default extra checksums.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Store{client: client, bucket: cfg.Bucket}, nil
}

// EnsureBucket creates the bucket if it doesn't exist. For local dev and tests.
func (s *Store) EnsureBucket(ctx context.Context) error {
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &s.bucket}); err == nil {
		return nil
	}
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucket})
	return err
}

// PutObject stores a small object in one request (e.g. the JPEG preview).
// Overwriting the same key is idempotent, which makes retries safe.
func (s *Store) PutObject(ctx context.Context, key, contentType string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   &contentType,
	})
	return err
}

// Stat reports whether a finished object exists at key, and its size.
func (s *Store) Stat(ctx context.Context, key string) (size int64, exists bool, err error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if isNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return aws.ToInt64(out.ContentLength), true, nil
}

// Progress describes how much of an upload is durably stored.
type Progress struct {
	// Complete means the finished object exists at the key.
	Complete bool
	// Committed is the byte offset to resume from (always a part boundary),
	// or the object size when Complete.
	Committed int64
}

func (s *Store) Progress(ctx context.Context, key string) (Progress, error) {
	size, exists, err := s.Stat(ctx, key)
	if err != nil {
		return Progress{}, err
	}
	if exists {
		return Progress{Complete: true, Committed: size}, nil
	}
	uploadID, found, err := s.findUpload(ctx, key)
	if err != nil || !found {
		return Progress{}, err
	}
	parts, err := s.listParts(ctx, key, uploadID)
	if err != nil {
		return Progress{}, err
	}
	_, committed := contiguous(parts, PartSize, -1)
	return Progress{Committed: committed}, nil
}

// Resume finds the in-flight multipart upload for key, or starts one, and
// returns a handle positioned after the contiguous parts already stored.
func (s *Store) Resume(ctx context.Context, key, contentType string, total int64) (*Upload, error) {
	uploadID, found, err := s.findUpload(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		out, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:      &s.bucket,
			Key:         &key,
			ContentType: &contentType,
		})
		if err != nil {
			return nil, fmt.Errorf("create multipart upload: %w", err)
		}
		return &Upload{store: s, key: key, uploadID: aws.ToString(out.UploadId)}, nil
	}

	parts, err := s.listParts(ctx, key, uploadID)
	if err != nil {
		return nil, err
	}
	keep, committed := contiguous(parts, PartSize, total)
	if committed > total {
		return nil, ErrSizeConflict
	}
	return &Upload{store: s, key: key, uploadID: uploadID, parts: keep, committed: committed}, nil
}

// SHA256 streams the stored object through SHA-256. Used to verify the
// assembled file end to end, including parts sent on earlier connections.
func (s *Store) SHA256(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	h := sha256.New()
	if _, err := io.Copy(h, out.Body); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	return err
}

// findUpload returns the in-flight multipart upload for key, if any. If there
// are several (shouldn't happen with one upload per frame at a time), the most
// recently started wins; leftovers are cleaned up by the bucket's
// AbortIncompleteMultipartUpload lifecycle rule.
func (s *Store) findUpload(ctx context.Context, key string) (string, bool, error) {
	out, err := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: &s.bucket,
		Prefix: &key,
	})
	if err != nil {
		return "", false, fmt.Errorf("list multipart uploads: %w", err)
	}
	var best string
	var bestAt time.Time
	for _, u := range out.Uploads {
		if aws.ToString(u.Key) != key {
			continue
		}
		if at := aws.ToTime(u.Initiated); best == "" || at.After(bestAt) {
			best, bestAt = aws.ToString(u.UploadId), at
		}
	}
	return best, best != "", nil
}

func (s *Store) listParts(ctx context.Context, key, uploadID string) ([]types.Part, error) {
	var parts []types.Part
	p := s3.NewListPartsPaginator(s.client, &s3.ListPartsInput{
		Bucket:   &s.bucket,
		Key:      &key,
		UploadId: &uploadID,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list parts: %w", err)
		}
		parts = append(parts, page.Parts...)
	}
	return parts, nil
}

// Upload is one in-flight multipart upload. Not safe for concurrent use; the
// server holds a per-frame lock while using it.
type Upload struct {
	store     *Store
	key       string
	uploadID  string
	parts     []types.CompletedPart
	committed int64
}

// Committed is the byte offset the next part starts at.
func (u *Upload) Committed() int64 { return u.committed }

// WritePart uploads the next part. Every part but the last must be exactly
// PartSize bytes.
func (u *Upload) WritePart(ctx context.Context, data []byte) error {
	n := int32(len(u.parts) + 1)
	out, err := u.store.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        &u.store.bucket,
		Key:           &u.key,
		UploadId:      &u.uploadID,
		PartNumber:    aws.Int32(n),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("upload part %d: %w", n, err)
	}
	u.parts = append(u.parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(n)})
	u.committed += int64(len(data))
	return nil
}

// Complete assembles the parts into the final object.
func (u *Upload) Complete(ctx context.Context) error {
	_, err := u.store.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &u.store.bucket,
		Key:             &u.key,
		UploadId:        &u.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func isNotFound(err error) bool {
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 404
}
