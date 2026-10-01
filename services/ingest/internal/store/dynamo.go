package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoTelemetry stores telemetry in DynamoDB.
//
// Why this collection and not the others: telemetry is append-only, always read
// as "this device over this time range", has no joins, is the highest write
// volume in the system and the lowest value per row. That is exactly a
// partition-key/sort-key table, so it needs no index and never scans.
//
// Key design:
//
//	PK = device_id
//	SK = recorded_at as ISO-8601 UTC — lexicographic order is chronological, so
//	     a range query is Query ... BETWEEN :from AND :to with no GSI.
//
// The same (device, timestamp) written twice overwrites rather than duplicating,
// which makes a retried write and a re-run backfill both idempotent.
//
// Hot partitions: one device's writes all land on one partition. Fine at a few
// cameras; at scale the key would carry a time bucket (device#2026-09) to spread
// them, at the cost of querying across buckets.
type DynamoTelemetry struct {
	client *dynamodb.Client
	table  string
	ttl    time.Duration
}

type DynamoConfig struct {
	// Endpoint is empty for real AWS, or http://localhost:8000 for DynamoDB Local.
	Endpoint  string
	Region    string
	Table     string
	AccessKey string
	SecretKey string
	// TTL is how long a reading is kept. DynamoDB expires rows itself, so
	// retention costs no writes and needs no cron job.
	TTL time.Duration
}

func NewDynamoTelemetry(ctx context.Context, cfg DynamoConfig) (*DynamoTelemetry, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 90 * 24 * time.Hour
	}
	return &DynamoTelemetry{client: client, table: cfg.Table, ttl: ttl}, nil
}

// EnsureTable creates the table if it is missing. Real deployments would create
// it from Terraform; this keeps local runs and tests self-contained.
func (d *DynamoTelemetry) EnsureTable(ctx context.Context) error {
	_, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &d.table})
	if err == nil {
		return nil
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("describe table: %w", err)
	}

	_, err = d.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &d.table,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("device_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("recorded_at"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("device_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("recorded_at"), KeyType: types.KeyTypeRange},
		},
		// On-demand: this workload is ~0.5 writes/second with bursts, so paying
		// per request beats provisioning capacity for a peak that rarely comes.
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	waiter := dynamodb.NewTableExistsWaiter(d.client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: &d.table}, time.Minute); err != nil {
		return fmt.Errorf("wait for table: %w", err)
	}

	// TTL is best-effort: DynamoDB Local accepts it, and on real AWS it is what
	// makes retention free. A failure here shouldn't stop the service starting.
	_, _ = d.client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: &d.table,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expires_at"),
			Enabled:       aws.Bool(true),
		},
	})
	return nil
}

// timeKey formats a sort key. RFC3339 with nanoseconds in UTC sorts
// lexicographically in chronological order, which is the whole trick.
func timeKey(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (d *DynamoTelemetry) InsertTelemetry(ctx context.Context, t TelemetryReading) error {
	item := map[string]types.AttributeValue{
		"device_id":   &types.AttributeValueMemberS{Value: t.DeviceID},
		"recorded_at": &types.AttributeValueMemberS{Value: timeKey(t.RecordedAt)},
		"received_at": &types.AttributeValueMemberS{Value: timeKey(time.Now())},
		"expires_at": &types.AttributeValueMemberN{
			Value: strconv.FormatInt(t.RecordedAt.Add(d.ttl).Unix(), 10)},
	}
	putNumber(item, "temperature_c", t.TemperatureC)
	putNumber(item, "humidity_pct", t.HumidityPct)
	putNumber(item, "probe_temp_c", t.ProbeTempC)

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &d.table, Item: item})
	if err != nil {
		return fmt.Errorf("dynamo put telemetry: %w", err)
	}
	return nil
}

// QueryRange answers the one question this table was designed for.
func (d *DynamoTelemetry) QueryRange(ctx context.Context, deviceID string, from, to time.Time, limit int32) ([]TelemetryReading, error) {
	out, err := d.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &d.table,
		KeyConditionExpression: aws.String("device_id = :d AND recorded_at BETWEEN :from AND :to"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":d":    &types.AttributeValueMemberS{Value: deviceID},
			":from": &types.AttributeValueMemberS{Value: timeKey(from)},
			":to":   &types.AttributeValueMemberS{Value: timeKey(to)},
		},
		ScanIndexForward: aws.Bool(false), // newest first, like the GUI wants
		Limit:            aws.Int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamo query telemetry: %w", err)
	}

	readings := make([]TelemetryReading, 0, len(out.Items))
	for _, item := range out.Items {
		r := TelemetryReading{DeviceID: deviceID}
		if v, ok := item["recorded_at"].(*types.AttributeValueMemberS); ok {
			if ts, err := time.Parse(time.RFC3339Nano, v.Value); err == nil {
				r.RecordedAt = ts
			}
		}
		r.TemperatureC = readNumber(item, "temperature_c")
		r.HumidityPct = readNumber(item, "humidity_pct")
		r.ProbeTempC = readNumber(item, "probe_temp_c")
		readings = append(readings, r)
	}
	return readings, nil
}

// CountDevice counts a device's rows. Used to verify a backfill; a Query with
// Select=COUNT stays on the key, so it never degenerates into a table scan.
func (d *DynamoTelemetry) CountDevice(ctx context.Context, deviceID string) (int32, error) {
	var total int32
	var startKey map[string]types.AttributeValue
	for {
		out, err := d.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &d.table,
			KeyConditionExpression: aws.String("device_id = :d"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":d": &types.AttributeValueMemberS{Value: deviceID},
			},
			Select:            types.SelectCount,
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return 0, fmt.Errorf("dynamo count: %w", err)
		}
		total += out.Count
		if len(out.LastEvaluatedKey) == 0 {
			return total, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// BatchInsert writes up to 25 readings per request — DynamoDB's batch limit —
// and retries whatever the service declines to process.
func (d *DynamoTelemetry) BatchInsert(ctx context.Context, readings []TelemetryReading) error {
	const batchSize = 25
	for start := 0; start < len(readings); start += batchSize {
		end := min(start+batchSize, len(readings))
		writes := make([]types.WriteRequest, 0, end-start)
		for _, r := range readings[start:end] {
			item := map[string]types.AttributeValue{
				"device_id":   &types.AttributeValueMemberS{Value: r.DeviceID},
				"recorded_at": &types.AttributeValueMemberS{Value: timeKey(r.RecordedAt)},
				"received_at": &types.AttributeValueMemberS{Value: timeKey(time.Now())},
				"expires_at": &types.AttributeValueMemberN{
					Value: strconv.FormatInt(r.RecordedAt.Add(d.ttl).Unix(), 10)},
			}
			putNumber(item, "temperature_c", r.TemperatureC)
			putNumber(item, "humidity_pct", r.HumidityPct)
			putNumber(item, "probe_temp_c", r.ProbeTempC)
			writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		}

		unprocessed := map[string][]types.WriteRequest{d.table: writes}
		for attempt := 0; len(unprocessed) > 0; attempt++ {
			if attempt == 5 {
				return errors.New("dynamo batch write: still unprocessed after 5 attempts")
			}
			if attempt > 0 {
				time.Sleep(time.Duration(attempt*100) * time.Millisecond) // throttle backoff
			}
			out, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: unprocessed})
			if err != nil {
				return fmt.Errorf("dynamo batch write: %w", err)
			}
			unprocessed = out.UnprocessedItems
		}
	}
	return nil
}

func putNumber(item map[string]types.AttributeValue, key string, v *float64) {
	if v == nil {
		return // absent, not zero: DynamoDB items are sparse by nature
	}
	item[key] = &types.AttributeValueMemberN{Value: strconv.FormatFloat(*v, 'f', -1, 64)}
}

func readNumber(item map[string]types.AttributeValue, key string) *float64 {
	v, ok := item[key].(*types.AttributeValueMemberN)
	if !ok {
		return nil
	}
	f, err := strconv.ParseFloat(v.Value, 64)
	if err != nil {
		return nil
	}
	return &f
}
