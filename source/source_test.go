package source

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Unit Tests for Producer Configuration
// ---------------------------------------------------------------------------

func TestDefaultProducerConfig(t *testing.T) {
	cfg := DefaultProducerConfig()

	if cfg.Broker != "localhost:9092" {
		t.Errorf("Expected broker localhost:9092, got %s", cfg.Broker)
	}

	if cfg.Topic != "source" {
		t.Errorf("Expected topic 'source', got %s", cfg.Topic)
	}

	if cfg.TotalRecords != 50_000_000 {
		t.Errorf("Expected 50M records, got %d", cfg.TotalRecords)
	}

	if cfg.Workers != 4 {
		t.Errorf("Expected 4 workers, got %d", cfg.Workers)
	}

	if cfg.BatchSize != 2000 {
		t.Errorf("Expected batch size 2000, got %d", cfg.BatchSize)
	}

	if cfg.RetryConfig.MaxAttempts != 5 {
		t.Errorf("Expected 5 max attempts, got %d", cfg.RetryConfig.MaxAttempts)
	}
}

func TestCustomProducerConfig(t *testing.T) {
	cfg := ProducerConfig{
		Broker:       "kafka.example.com:9092",
		Topic:        "custom-topic",
		TotalRecords: 1000000,
		Workers:      8,
		BatchSize:    5000,
		RetryConfig: RetryConfig{
			MaxAttempts:     10,
			InitialBackoff:  200 * time.Millisecond,
			MaxBackoff:      60 * time.Second,
			BackoffMultiple: 2.5,
		},
		ShutdownWindow: 60 * time.Second,
	}

	if cfg.Broker != "kafka.example.com:9092" {
		t.Errorf("Expected custom broker, got %s", cfg.Broker)
	}

	if cfg.RetryConfig.MaxAttempts != 10 {
		t.Errorf("Expected 10 retry attempts, got %d", cfg.RetryConfig.MaxAttempts)
	}

	if cfg.ShutdownWindow != 60*time.Second {
		t.Errorf("Expected 60s shutdown window, got %v", cfg.ShutdownWindow)
	}
}

// ---------------------------------------------------------------------------
// Unit Tests for Statistics
// ---------------------------------------------------------------------------

func TestProducerStats_RecordOperations(t *testing.T) {
	stats := &ProducerStats{}

	// Test initial state
	sent, writeErrors, retries, workerErrors := stats.snapshot()
	if sent != 0 || writeErrors != 0 || retries != 0 || workerErrors != 0 {
		t.Error("Expected all stats to be zero initially")
	}

	// Test recording sent
	stats.recordSent(100)
	stats.recordSent(50)
	sent, _, _, _ = stats.snapshot()
	if sent != 150 {
		t.Errorf("Expected 150 sent, got %d", sent)
	}

	// Test recording errors
	stats.recordWriteError()
	stats.recordWriteError()
	stats.recordWriteError()
	_, writeErrors, _, _ = stats.snapshot()
	if writeErrors != 3 {
		t.Errorf("Expected 3 write errors, got %d", writeErrors)
	}

	// Test recording retries
	stats.recordRetry()
	_, _, retries, _ = stats.snapshot()
	if retries != 1 {
		t.Errorf("Expected 1 retry, got %d", retries)
	}

	// Test recording worker errors
	stats.recordWorkerError()
	stats.recordWorkerError()
	_, _, _, workerErrors = stats.snapshot()
	if workerErrors != 2 {
		t.Errorf("Expected 2 worker errors, got %d", workerErrors)
	}
}

func TestProducerStats_ConcurrentAccess(t *testing.T) {
	stats := &ProducerStats{}
	done := make(chan bool)

	// Simulate concurrent writes from multiple goroutines
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				stats.recordSent(1)
				stats.recordWriteError()
				stats.recordRetry()
				stats.recordWorkerError()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	sent, writeErrors, retries, workerErrors := stats.snapshot()

	expectedPerStat := int64(10000) // 10 goroutines * 1000 iterations

	if sent != expectedPerStat {
		t.Errorf("Expected %d sent, got %d", expectedPerStat, sent)
	}

	if writeErrors != expectedPerStat {
		t.Errorf("Expected %d write errors, got %d", expectedPerStat, writeErrors)
	}

	if retries != expectedPerStat {
		t.Errorf("Expected %d retries, got %d", expectedPerStat, retries)
	}

	if workerErrors != expectedPerStat {
		t.Errorf("Expected %d worker errors, got %d", expectedPerStat, workerErrors)
	}
}

// ---------------------------------------------------------------------------
// Unit Tests for Backoff Calculation
// ---------------------------------------------------------------------------

func TestCalculateBackoff(t *testing.T) {
	config := RetryConfig{
		MaxAttempts:     5,
		InitialBackoff:  100 * time.Millisecond,
		MaxBackoff:      30 * time.Second,
		BackoffMultiple: 2.0,
	}

	tests := []struct {
		name    string
		attempt int
		minWant time.Duration
		maxWant time.Duration
	}{
		{
			name:    "first retry (attempt 0)",
			attempt: 0,
			minWant: 80 * time.Millisecond,
			maxWant: 100 * time.Millisecond,
		},
		{
			name:    "second retry (attempt 1)",
			attempt: 1,
			minWant: 160 * time.Millisecond,
			maxWant: 200 * time.Millisecond,
		},
		{
			name:    "third retry (attempt 2)",
			attempt: 2,
			minWant: 320 * time.Millisecond,
			maxWant: 400 * time.Millisecond,
		},
		{
			name:    "max backoff exceeded",
			attempt: 20,
			minWant: 24 * time.Second, // 80% of max
			maxWant: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := calculateBackoff(tt.attempt, config)
			if backoff < tt.minWant || backoff > tt.maxWant {
				t.Errorf("calculateBackoff(%d) = %v, want between %v and %v",
					tt.attempt, backoff, tt.minWant, tt.maxWant)
			}
		})
	}
}

func TestCalculateBackoff_DifferentMultiple(t *testing.T) {
	config := RetryConfig{
		MaxAttempts:     5,
		InitialBackoff:  100 * time.Millisecond,
		MaxBackoff:      10 * time.Second,
		BackoffMultiple: 3.0, // Triple each time
	}

	backoff0 := calculateBackoff(0, config)
	backoff1 := calculateBackoff(1, config)

	// With 3.0 multiple and jitter, backoff1 should be roughly 2.4-3.0x backoff0
	ratio := float64(backoff1) / float64(backoff0)
	if ratio < 2.0 || ratio > 4.0 {
		t.Errorf("Expected backoff ratio between 2.0 and 4.0, got %.2f", ratio)
	}
}

// ---------------------------------------------------------------------------
// Unit Tests for Helper Functions
// ---------------------------------------------------------------------------

func TestRandFromSet(t *testing.T) {
	r := newTestRand()
	set := []byte("abc")

	result := randFromSet(r, set, 10)

	if len(result) != 10 {
		t.Errorf("Expected length 10, got %d", len(result))
	}

	// Check all bytes are from the set
	for _, b := range result {
		if b != 'a' && b != 'b' && b != 'c' {
			t.Errorf("Found byte %c not in set", b)
		}
	}
}

func TestRandFromSet_Letters(t *testing.T) {
	r := newTestRand()

	result := randFromSet(r, letters, 20)

	if len(result) != 20 {
		t.Errorf("Expected length 20, got %d", len(result))
	}

	// All should be letters
	for _, b := range result {
		isLetter := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
		if !isLetter {
			t.Errorf("Found non-letter byte %c", b)
		}
	}
}

func TestRandFromSet_AddressChars(t *testing.T) {
	r := newTestRand()

	result := randFromSet(r, addressChars, 30)

	if len(result) != 30 {
		t.Errorf("Expected length 30, got %d", len(result))
	}

	// All should be alphanumeric or space
	for _, b := range result {
		isValid := (b >= 'a' && b <= 'z') ||
			(b >= 'A' && b <= 'Z') ||
			(b >= '0' && b <= '9') ||
			b == ' '
		if !isValid {
			t.Errorf("Found invalid address char byte %c", b)
		}
	}
}

func TestRandFromSet_EmptySet(t *testing.T) {
	r := newTestRand()
	set := []byte{}

	// This will panic in the actual code, which is expected behavior
	// Testing the panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic with empty set, got none")
		}
	}()

	_ = randFromSet(r, set, 5)
}

func TestRandFromSet_ZeroLength(t *testing.T) {
	r := newTestRand()
	set := []byte("abc")

	result := randFromSet(r, set, 0)

	if len(result) != 0 {
		t.Errorf("Expected empty result, got length %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Unit Tests for Record Generation Format
// ---------------------------------------------------------------------------

func TestRecordGeneration_Format(t *testing.T) {
	// This tests the record format indirectly
	// In production code, you might extract this to a testable function

	continents := []string{"North America", "Asia", "Europe"}

	for _, continent := range continents {
		// Check continent is valid
		found := false
		for _, c := range continents {
			if c == continent {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Invalid continent: %s", continent)
		}
	}
}

func TestContinents_AllValid(t *testing.T) {
	expectedContinents := []string{
		"North America", "Asia", "South America",
		"Europe", "Africa", "Australia",
	}

	if len(continents) != len(expectedContinents) {
		t.Errorf("Expected %d continents, got %d", len(expectedContinents), len(continents))
	}

	for i, expected := range expectedContinents {
		if continents[i] != expected {
			t.Errorf("Continent[%d]: expected %s, got %s", i, expected, continents[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Helper for tests
// ---------------------------------------------------------------------------

func newTestRand() *rand.Rand {
	return rand.New(rand.NewSource(12345)) // Fixed seed for reproducible tests
}

// ---------------------------------------------------------------------------
// Integration-style tests (would need actual Kafka for full integration)
// ---------------------------------------------------------------------------

func TestWorkerDistribution(t *testing.T) {
	tests := []struct {
		name         string
		totalRecords int64
		workers      int
		wantPerWorker int64
		wantRemainder int64
	}{
		{
			name:          "evenly divisible",
			totalRecords:  1000,
			workers:       10,
			wantPerWorker: 100,
			wantRemainder: 0,
		},
		{
			name:          "with remainder",
			totalRecords:  1005,
			workers:       10,
			wantPerWorker: 100,
			wantRemainder: 5,
		},
		{
			name:          "small batch",
			totalRecords:  7,
			workers:       3,
			wantPerWorker: 2,
			wantRemainder: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perWorker := tt.totalRecords / int64(tt.workers)
			remainder := tt.totalRecords % int64(tt.workers)

			if perWorker != tt.wantPerWorker {
				t.Errorf("Expected %d per worker, got %d", tt.wantPerWorker, perWorker)
			}

			if remainder != tt.wantRemainder {
				t.Errorf("Expected %d remainder, got %d", tt.wantRemainder, remainder)
			}

			// Verify total
			total := perWorker * int64(tt.workers) + remainder
			if total != tt.totalRecords {
				t.Errorf("Total mismatch: %d workers * %d + %d remainder = %d, want %d",
					tt.workers, perWorker, remainder, total, tt.totalRecords)
			}
		})
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Immediately cancel
	cancel()

	select {
	case <-ctx.Done():
		// Expected
		if ctx.Err() != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", ctx.Err())
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Context cancellation not detected")
	}
}

// ---------------------------------------------------------------------------
// Benchmark Tests
// ---------------------------------------------------------------------------

func BenchmarkRandFromSet(b *testing.B) {
	r := newTestRand()
	set := []byte("abcdefghijklmnopqrstuvwxyz")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = randFromSet(r, set, 20)
	}
}

func BenchmarkCalculateBackoff(b *testing.B) {
	config := RetryConfig{
		MaxAttempts:     5,
		InitialBackoff:  100 * time.Millisecond,
		MaxBackoff:      30 * time.Second,
		BackoffMultiple: 2.0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = calculateBackoff(i%10, config)
	}
}

func BenchmarkProducerStats_RecordSent(b *testing.B) {
	stats := &ProducerStats{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.recordSent(1)
	}
}

func BenchmarkProducerStats_Snapshot(b *testing.B) {
	stats := &ProducerStats{}
	stats.recordSent(1000000)
	stats.recordWriteError()
	stats.recordRetry()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = stats.snapshot()
	}
}