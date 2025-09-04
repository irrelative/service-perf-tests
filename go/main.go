package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
)

type envCfg struct {
	AWSProfile string
	AWSRegion  string

	Bucket   string
	Prefix   string
	Steps    int
	PayloadMB int
	Serializer string
	Cleanup   bool
}

func loadEnv() envCfg {
	_ = godotenv.Load() // best-effort

	cfg := envCfg{}
	cfg.AWSProfile = os.Getenv("AWS_PROFILE")
	cfg.AWSRegion = os.Getenv("AWS_REGION")

	cfg.Bucket = os.Getenv("ORCH_BUCKET")
	if v := os.Getenv("ORCH_PREFIX"); v != "" {
		cfg.Prefix = v
	} else {
		cfg.Prefix = "orchestration-bench/runs"
	}

	cfg.Steps = getenvInt("STEPS", 5)
	cfg.PayloadMB = getenvInt("PAYLOAD_MB", 50)
	cfg.Serializer = strings.ToLower(getenvDefault("SERIALIZER", "json"))
	cfg.Cleanup = getenvBool("CLEANUP", false)

	return cfg
}

func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		if err == nil {
			return n
		}
	}
	return def
}

func getenvBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "t", "yes", "y", "on":
			return true
		default:
			return false
		}
	}
	return def
}

func makeS3Client(ctx context.Context, profile, region string) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	awscfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(awscfg), nil
}

func buildS3URI(bucket, key string) string {
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}

func serializerExt(serializer string) (string, error) {
	switch serializer {
	case "json":
		return "json", nil
	default:
		return "", fmt.Errorf("unsupported serializer: %s", serializer)
	}
}

func serializePayload(data []byte, serializer string) (body []byte, contentType string, seconds float64, err error) {
	start := time.Now()
	switch serializer {
	case "json":
		b64 := base64.StdEncoding.EncodeToString(data)
		obj := map[string]any{
			"type":     "bytes",
			"encoding": "base64",
			"data":     b64,
		}
		body, err = json.Marshal(obj)
		if err != nil {
			return nil, "", 0, err
		}
		contentType = "application/json"
	default:
		return nil, "", 0, fmt.Errorf("unsupported serializer: %s", serializer)
	}
	return body, contentType, time.Since(start).Seconds(), nil
}

func deserializePayload(body []byte, serializer string) (payload []byte, seconds float64, err error) {
	start := time.Now()
	switch serializer {
	case "json":
		var obj map[string]any
		if err := json.Unmarshal(body, &obj); err != nil {
			return nil, 0, err
		}
		enc, _ := obj["encoding"].(string)
		if enc != "base64" {
			return nil, 0, fmt.Errorf("unexpected JSON payload shape (expected base64-encoded object)")
		}
		dataStr, _ := obj["data"].(string)
		payload, err = base64.StdEncoding.DecodeString(dataStr)
		if err != nil {
			return nil, 0, err
		}
	default:
		return nil, 0, fmt.Errorf("unsupported serializer: %s", serializer)
	}
	return payload, time.Since(start).Seconds(), nil
}

func s3Put(ctx context.Context, client *s3.Client, bucket, key string, body []byte, contentType string, metadata map[string]string) (seconds float64, err error) {
	start := time.Now()
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      ptr(bucket),
		Key:         ptr(key),
		Body:        bytes.NewReader(body),
		ContentType: ptr(contentType),
		Metadata:    metadata,
	})
	return time.Since(start).Seconds(), err
}

func s3Get(ctx context.Context, client *s3.Client, bucket, key string) (body []byte, metadata map[string]string, seconds float64, err error) {
	start := time.Now()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr(bucket),
		Key:    ptr(key),
	})
	if err != nil {
		return nil, nil, 0, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, 0, err
	}
	md := map[string]string{}
	for k, v := range out.Metadata {
		md[k] = v
	}
	return b, md, time.Since(start).Seconds(), nil
}

func noopTransform(payload []byte) (out []byte, seconds float64) {
	start := time.Now()
	if len(payload) > 0 {
		_ = payload[0]
	}
	return payload, time.Since(start).Seconds()
}

func ensureOutDir() (string, error) {
	outDir := filepath.Join(".", "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	return outDir, nil
}

// ptr returns a pointer to the provided value (generic helper).
func ptr[T any](v T) *T { return &v }

func writeMetricsLine(runID, path string, record map[string]any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	rec := map[string]any{"run_id": runID}
	for k, v := range record {
		rec[k] = v
	}
	enc, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	enc = append(enc, '\n')
	_, err = f.Write(enc)
	return err
}

func orchestrateChain(ctx context.Context, s3c *s3.Client, bucket, prefix string, steps, payloadMB int, serializer, runID string, cleanup bool) error {
	outDir, err := ensureOutDir()
	if err != nil {
		return err
	}
	metricsPath := filepath.Join(outDir, fmt.Sprintf("metrics-%s.jsonl", runID))

	// Seed initial payload
	payloadSize := payloadMB * 1024 * 1024
	seedBytes := make([]byte, payloadSize)
	if _, err := io.ReadFull(rand.Reader, seedBytes); err != nil {
		return fmt.Errorf("failed to generate random payload: %w", err)
	}
	ext, err := serializerExt(serializer)
	if err != nil {
		return err
	}
	seedKey := fmt.Sprintf("%s/step-000.%s", strings.TrimRight(prefix, "/"), ext)
	seedURI := buildS3URI(bucket, seedKey)

	seedBody, seedCType, seedSerializeS, err := serializePayload(seedBytes, serializer)
	if err != nil {
		return err
	}
	seedUploadS, err := s3Put(ctx, s3c, bucket, seedKey, seedBody, seedCType, map[string]string{
		"serializer": serializer,
		"run_id":     runID,
		"step":       "0",
	})
	if err != nil {
		return err
	}

	_ = writeMetricsLine(runID, metricsPath, map[string]any{
		"phase":          "seed",
		"step_index":     0,
		"output_s3_uri":  seedURI,
		"bytes_uploaded": len(seedBody),
		"serialize_s":    round6(seedSerializeS),
		"upload_s":       round6(seedUploadS),
		"payload_mb":     payloadMB,
		"serializer":     serializer,
	})
	fmt.Printf("[seed] uploaded initial payload to %s (%d MB, serializer=%s)\n", seedURI, payloadMB, serializer)

	producedKeys := []string{seedKey}
	prevKey := seedKey
	prevURI := seedURI

	// Steps 1..N
	for i := 1; i <= steps; i++ {
		stepStart := time.Now()

		bodyBytes, metadata, downloadS, err := s3Get(ctx, s3c, bucket, prevKey)
		if err != nil {
			return err
		}
		metaSerializer := strings.ToLower(metadata["serializer"])
		currentSerializer := serializer
		if metaSerializer != "" && metaSerializer != serializer {
			currentSerializer = metaSerializer
		}

		payloadBytes, deserializeS, err := deserializePayload(bodyBytes, currentSerializer)
		if err != nil {
			return err
		}

		transformedBytes, transformS := noopTransform(payloadBytes)

		outBody, outCType, serializeS, err := serializePayload(transformedBytes, currentSerializer)
		if err != nil {
			return err
		}

		nextKey := fmt.Sprintf("%s/step-%03d.%s", strings.TrimRight(prefix, "/"), i, ext)
		nextURI := buildS3URI(bucket, nextKey)
		uploadS, err := s3Put(ctx, s3c, bucket, nextKey, outBody, outCType, map[string]string{
			"serializer": currentSerializer,
			"run_id":     runID,
			"step":       fmt.Sprintf("%d", i),
		})
		if err != nil {
			return err
		}
		stepWallS := time.Since(stepStart).Seconds()

		_ = writeMetricsLine(runID, metricsPath, map[string]any{
			"phase":            "step",
			"step_index":       i,
			"input_s3_uri":     prevURI,
			"output_s3_uri":    nextURI,
			"bytes_downloaded": len(bodyBytes),
			"bytes_uploaded":   len(outBody),
			"download_s":       round6(downloadS),
			"deserialize_s":    round6(deserializeS),
			"transform_s":      round6(transformS),
			"serialize_s":      round6(serializeS),
			"upload_s":         round6(uploadS),
			"step_wall_s":      round6(stepWallS),
			"serializer":       currentSerializer,
		})

		fmt.Printf("[step %d/%d] %s -> %s | dl=%.3fs de=%.3fs tr=%.3fs se=%.3fs ul=%.3fs wall=%.3fs\n",
			i, steps, prevURI, nextURI, downloadS, deserializeS, transformS, serializeS, uploadS, stepWallS)

		producedKeys = append(producedKeys, nextKey)
		prevKey = nextKey
		prevURI = nextURI
	}

	fmt.Printf("Run complete. Metrics written to %s\n", metricsPath)

	if cleanup {
		toDelete := make([]s3types.ObjectIdentifier, 0, len(producedKeys))
		for _, k := range producedKeys {
			key := k
			toDelete = append(toDelete, s3types.ObjectIdentifier{Key: ptr(key)})
		}
		if err := s3DeleteObjects(ctx, s3c, bucket, toDelete); err != nil {
			return err
		}
		fmt.Printf("Cleanup complete. Deleted %d objects from s3://%s/%s/\n", len(producedKeys), bucket, strings.TrimRight(prefix, "/"))
	}

	return nil
}

func s3DeleteObjects(ctx context.Context, client *s3.Client, bucket string, objs []s3types.ObjectIdentifier) error {
	if len(objs) == 0 {
		return nil
	}
	_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: ptr(bucket),
		Delete: &s3types.Delete{
			Objects: objs,
			Quiet:   ptr(true),
		},
	})
	return err
}

func round6(x float64) float64 {
	return math.Round(x*1e6) / 1e6
}

func computeStats(xs []float64) (min, max, avg, stdev, median float64) {
	if len(xs) == 0 {
		return 0, 0, 0, 0, 0
	}
	min, max = xs[0], xs[0]
	sum := 0.0
	for _, v := range xs {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += v
	}
	avg = sum / float64(len(xs))
	// sample standard deviation (like Python statistics.stdev)
	if len(xs) >= 2 {
		variance := 0.0
		for _, v := range xs {
			d := v - avg
			variance += d * d
		}
		variance /= float64(len(xs) - 1)
		stdev = math.Sqrt(variance)
	} else {
		stdev = 0
	}
	// median
	cp := make([]float64, len(xs))
	copy(cp, xs)
	// simple insertion sort for small N
	for i := 1; i < len(cp); i++ {
		j := i
		for j > 0 && cp[j-1] > cp[j] {
			cp[j-1], cp[j] = cp[j], cp[j-1]
			j--
		}
	}
	n := len(cp)
	if n%2 == 1 {
		median = cp[n/2]
	} else {
		median = (cp[n/2-1] + cp[n/2]) / 2
	}
	return
}

func genRunID() string {
	ts := time.Now().UTC().Format("20060102-150405")
	// 8 hex chars
	var b [4]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return fmt.Sprintf("%s-%x", ts, b)
}

func main() {
	env := loadEnv()

	var (
		flagBucket     = flag.String("bucket", env.Bucket, "S3 bucket for payloads (required if not in env)")
		flagPrefix     = flag.String("prefix", env.Prefix, "S3 prefix for this run (default: orchestration-bench/runs)")
		flagSteps      = flag.Int("steps", env.Steps, "Number of steps in the chain (default: 5)")
		flagPayloadMB  = flag.Int("payload-mb", env.PayloadMB, "Payload size in MB (default: 50)")
		flagSerializer = flag.String("serializer", "json", "Serialization format (only: json)")
		flagCleanup    = flag.Bool("cleanup", env.Cleanup, "Delete S3 objects after run")
		flagRunID      = flag.String("run-id", "", "Run identifier (default: auto)")
		flagAWSProfile = flag.String("aws-profile", env.AWSProfile, "AWS profile name (optional)")
		flagAWSRegion  = flag.String("aws-region", env.AWSRegion, "AWS region (optional; SDK default if blank)")
		flagRepeats    = flag.Int("repeats", 10, "Repeat the run this many times to compute summary stats (default: 10)")
	)
	flag.Parse()

	if *flagBucket == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --bucket (or ORCH_BUCKET) is required.")
		os.Exit(2)
	}

	runID := *flagRunID
	if runID == "" {
		runID = genRunID()
	}
	basePrefix := strings.TrimRight(*flagPrefix, "/") + "/" + runID

	ctx := context.Background()
	s3c, err := makeS3Client(ctx, *flagAWSProfile, *flagAWSRegion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: failed to create S3 client: %v\n", err)
		os.Exit(3)
	}

	walls := make([]float64, 0, *flagRepeats)
	for i := 0; i < *flagRepeats; i++ {
		childRunID := runID
		repeatPrefix := basePrefix
		if *flagRepeats > 1 {
			childRunID = fmt.Sprintf("%s-r%02d", runID, i+1)
			repeatPrefix = basePrefix + "/" + childRunID
		}

		t0 := time.Now()
		err := orchestrateChain(ctx, s3c, *flagBucket, repeatPrefix, *flagSteps, *flagPayloadMB, *flagSerializer, childRunID, *flagCleanup)
		wall := time.Since(t0).Seconds()
		walls = append(walls, wall)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR during orchestration: %v\n", err)
			os.Exit(4)
		}
		fmt.Printf("End-to-end wall time: %.3fs (run_id=%s)\n", wall, childRunID)
	}

	if len(walls) > 0 {
		minS, maxS, avgS, stdevS, medianS := computeStats(walls)
		fmt.Printf("Summary over %d runs (base run_id=%s): min=%.3fs max=%.3fs avg=%.3fs stdev=%.3fs median=%.3fs\n",
			len(walls), runID, minS, maxS, avgS, stdevS, medianS)

		outDir, err := ensureOutDir()
		if err == nil {
			summaryPath := filepath.Join(outDir, fmt.Sprintf("summary-%s.json", runID))
			_ = writeJSON(summaryPath, map[string]any{
				"base_run_id": runID,
				"repeats":     len(walls),
				"times_s":     round6Slice(walls),
				"min_s":       round6(minS),
				"max_s":       round6(maxS),
				"avg_s":       round6(avgS),
				"stdev_s":     round6(stdevS),
				"median_s":    round6(medianS),
			})
			fmt.Printf("Wrote summary to %s\n", summaryPath)
		}
	}
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func round6Slice(xs []float64) []float64 {
	out := make([]float64, len(xs))
	for i, v := range xs {
		out[i] = round6(v)
	}
	return out
}
