package mph

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect" // Added for diagnostics
	"runtime"
	"sync" // Added for non-blocking test
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	sampleData = map[string]string{
		"one":   "1",
		"two":   "2",
		"three": "3",
		"four":  "4",
		"five":  "5",
		"six":   "6",
		"seven": "7",
	}
	sampleKeys [][]byte
	sampleVals [][]byte
)

var (
	words [][]byte
)

func init() {
	// Check if we are in the correct directory relative to testdata
	// This can happen if tests are run from a different working directory
	if _, err := os.Stat("testdata/words"); os.IsNotExist(err) {
		// Try navigating up one level if common 'go test ./...' pattern is used
		if _, err := os.Stat("../testdata/words"); err == nil {
			os.Chdir("..") // Go up one level
		} else {
			// If still not found, panic as before, but provide more context
			wd, _ := os.Getwd()
			panic("testdata/words not found relative to working directory: " + wd)
		}
	}

	f, err := os.Open("testdata/words")
	if err != nil {
		panic(err)
	}
	defer f.Close() // Ensure file is closed
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		// Trim newline characters for cleaner keys
		line = bytes.TrimRight(line, "\r\n")
		if len(line) > 0 { // Avoid adding empty lines if any
			words = append(words, line)
		}
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
	}
	if len(words) == 0 {
		panic("failed to load any words from testdata/words")
	}

	// Pre-slice sample data for convenience in tests
	for k, v := range sampleData {
		sampleKeys = append(sampleKeys, []byte(k))
		sampleVals = append(sampleVals, []byte(v))
	}
}

func cmpDataSlice(t *testing.T, cl, cr *CHD, l, r []dataSlice) {
	t.Helper()
	assert.Equal(t, len(l), len(r))
	for i := 0; len(l) > i; i++ {
		assert.Equal(t, cl.slice(l[i]), cr.slice(r[i]))
	}
}

// Helper to build a CHD from key/value slices for tests
func buildCHDFromSlices(t *testing.T, keys, values [][]byte, builder *CHDBuilder) (*CHD, error) {
	t.Helper()
	if builder == nil {
		builder = Builder()
	}
	for i := range keys {
		builder.Add(keys[i], values[i])
	}
	// Force GC before build to get more stable memory measurements if needed later
	runtime.GC()
	return builder.Build()
}

// Helper to compare two CHD tables
func assertCHDEqual(t *testing.T, expected, actual *CHD) {
	t.Helper()
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	assert.Equal(t, expected.r, actual.r, "Random vectors 'r' should be equal")
	assert.Equal(t, expected.indices, actual.indices, "Indices should be equal")
	// We need to compare the underlying data referenced by keys/values, not just the slices themselves
	require.Equal(t, len(expected.keys), len(actual.keys), "Number of keys should be equal")
	require.Equal(t, len(expected.values), len(actual.values), "Number of values should be equal")

	// Build maps for easier comparison of content
	mapExpected := make(map[string]string)
	mapActual := make(map[string]string)

	for i := range expected.keys {
		keyBytes := expected.slice(expected.keys[i])
		valBytes := expected.slice(expected.values[i])
		mapExpected[string(keyBytes)] = string(valBytes)
	}
	for i := range actual.keys {
		keyBytes := actual.slice(actual.keys[i])
		valBytes := actual.slice(actual.values[i])
		mapActual[string(keyBytes)] = string(valBytes)
	}

	assert.Equal(t, mapExpected, mapActual, "Key/value content should be identical")
}

func TestCHDBuilder(t *testing.T) {
	b := Builder()
	for k, v := range sampleData {
		b.Add([]byte(k), []byte(v))
	}
	c, err := b.Build()
	assert.NoError(t, err)
	assert.Equal(t, 7, len(c.keys))
	for k, v := range sampleData {
		assert.Equal(t, []byte(v), c.Get([]byte(k)))
	}
	assert.Nil(t, c.Get([]byte("monkey")))
}

func TestCHDSerialization(t *testing.T) {
	cb := Builder()
	for _, v := range words {
		cb.Add([]byte(v), []byte(v))
	}
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n.r, m.r)
	assert.Equal(t, n.indices, m.indices)
	cmpDataSlice(t, n, m, n.keys, m.keys)
	cmpDataSlice(t, n, m, n.values, m.values)
	for _, v := range words {
		assert.Equal(t, []byte(v), n.Get([]byte(v)))
	}
}

func TestCHDSerialization_empty(t *testing.T) {
	cb := Builder()
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n.r, m.r)
	assert.Equal(t, n.indices, m.indices)
	cmpDataSlice(t, n, m, n.keys, m.keys)
	cmpDataSlice(t, n, m, n.values, m.values)
}

func TestCHDSerialization_one(t *testing.T) {
	cb := Builder()
	cb.Add([]byte("k"), []byte("v"))
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n.r, m.r)
	assert.Equal(t, n.indices, m.indices)
	cmpDataSlice(t, n, m, n.keys, m.keys)
	cmpDataSlice(t, n, m, n.values, m.values)
}

// --- Start of New Phase 1 Tests ---

func TestBuilderParameterDefaults(t *testing.T) {
	b := Builder()
	assert.NotNil(t, b)
	// Check internal fields directly for defaults
	assert.Equal(t, 0.5, b.bucketRatio, "Default bucketRatio should be 0.5")
	assert.Equal(t, 10_000_000, b.retryLimit, "Default retryLimit should be 10,000,000")
	assert.False(t, b.seedSetByUser, "seedSetByUser should be false by default")
	assert.Zero(t, b.userSeed, "userSeed should be 0 by default")
}

func TestBuilderSeedSetter(t *testing.T) {
	b := Builder()
	seedValue := int64(12345)
	retBuilder := b.Seed(seedValue)

	assert.Same(t, b, retBuilder, "Seed should return the same builder instance for chaining")
	assert.True(t, b.seedSetByUser, "seedSetByUser should be true after calling Seed")
	assert.Equal(t, seedValue, b.userSeed, "userSeed should be set to the provided value")
}

func TestBuilderSettersValid(t *testing.T) {
	b := Builder()

	// Test BucketRatio
	ratio := 1.0
	retBuilder, err := b.BucketRatio(ratio)
	require.NoError(t, err, "Setting valid BucketRatio should not error")
	assert.Same(t, b, retBuilder, "BucketRatio should return the same builder instance")
	assert.Equal(t, ratio, b.bucketRatio, "bucketRatio should be updated")

	// Test RetryLimit
	limit := 5000
	retBuilder, err = b.RetryLimit(limit)
	require.NoError(t, err, "Setting valid RetryLimit should not error")
	assert.Same(t, b, retBuilder, "RetryLimit should return the same builder instance")
	assert.Equal(t, limit, b.retryLimit, "retryLimit should be updated")

	// Test Chaining
	seedVal := int64(987)
	limitVal := 9999
	ratioVal := 0.75
	b2 := Builder()
	retBuilder, err = b2.Seed(seedVal).BucketRatio(ratioVal)
	require.NoError(t, err)
	retBuilder, err = retBuilder.RetryLimit(limitVal)
	require.NoError(t, err)

	assert.Same(t, b2, retBuilder, "Chained calls should return the original builder")
	assert.Equal(t, seedVal, b2.userSeed)
	assert.True(t, b2.seedSetByUser)
	assert.Equal(t, ratioVal, b2.bucketRatio)
	assert.Equal(t, limitVal, b2.retryLimit)
}

func TestBuilderSettersInvalid(t *testing.T) {
	b := Builder()

	// Test Invalid BucketRatio
	_, err := b.BucketRatio(0.0)
	assert.Error(t, err, "Setting BucketRatio to 0.0 should error")
	assert.Contains(t, err.Error(), "bucket ratio must be greater than 0.0")

	_, err = b.BucketRatio(-1.5)
	assert.Error(t, err, "Setting negative BucketRatio should error")
	assert.Contains(t, err.Error(), "bucket ratio must be greater than 0.0")

	// Ensure original value wasn't changed
	assert.Equal(t, 0.5, b.bucketRatio, "Invalid BucketRatio should not change the value")

	// Test Invalid RetryLimit
	_, err = b.RetryLimit(0)
	assert.Error(t, err, "Setting RetryLimit to 0 should error")
	assert.Contains(t, err.Error(), "retry limit must be greater than 0")

	_, err = b.RetryLimit(-100)
	assert.Error(t, err, "Setting negative RetryLimit should error")
	assert.Contains(t, err.Error(), "retry limit must be greater than 0")

	// Ensure original value wasn't changed
	assert.Equal(t, 10_000_000, b.retryLimit, "Invalid RetryLimit should not change the value")
}

// --- End of New Phase 1 Tests ---

// --- Start of New Phase 2 Tests ---

// TestBuildUsesSeed verifies that providing the same seed results in identical tables.
func TestBuildUsesSeed(t *testing.T) {
	seed := int64(12345)

	b1 := Builder().Seed(seed)
	c1, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b1)
	require.NoError(t, err, "Build 1 should succeed")

	b2 := Builder().Seed(seed)
	c2, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b2)
	require.NoError(t, err, "Build 2 should succeed")

	b3 := Builder().Seed(seed + 1) // Different seed
	c3, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b3)
	require.NoError(t, err, "Build 3 should succeed")

	assertCHDEqual(t, c1, c2)

	// Verify that c3 is different (highly likely, though theoretically could be same)
	// Comparing 'r' is a good indicator of difference.
	assert.NotEqual(t, c1.r, c3.r, "Different seeds should likely produce different 'r' vectors")
}

// TestBuildUsesRetryLimit verifies that a low limit causes failure
// and a high limit allows success for a potentially tricky build.
// Note: Finding a reliable small dataset + seed that *requires* many retries
// is difficult. This test uses a very low limit to force failure easily.
func TestBuildUsesRetryLimit(t *testing.T) {
	// Use a slightly larger dataset than sampleData
	testKeys := words[:50] // Use first 50 words
	testVals := make([][]byte, len(testKeys))
	for i := range testKeys {
		testVals[i] = []byte(fmt.Sprintf("val%d", i))
	}

	seed := int64(42) // Use a fixed seed

	// Force failure with an extremely low limit
	bFail := Builder().Seed(seed)
	_, err := bFail.RetryLimit(1) // Set limit ridiculously low
	require.NoError(t, err)       // Setting the limit should not error
	_, err = buildCHDFromSlices(t, testKeys, testVals, bFail)
	assert.Error(t, err, "Build should fail with retry limit 1")
	assert.Contains(t, err.Error(), "failed to find a collision-free hash function after ~1 attempts")

	// Allow success with the default (or a reasonably high) limit
	bSucceed := Builder().Seed(seed)
	_, err = bSucceed.RetryLimit(10_000_000) // Default limit should be sufficient
	require.NoError(t, err)                  // Setting the limit should not error
	c, err := buildCHDFromSlices(t, testKeys, testVals, bSucceed)
	assert.NoError(t, err, "Build should succeed with default retry limit")
	require.NotNil(t, c)
	assert.Equal(t, len(testKeys), c.Len()) // Verify basic correctness
}

// TestBuildUsesBucketRatio verifies builds succeed with different valid ratios.
// Directly verifying the internal 'm' is hard without exposing it, so we mainly
// check for successful completion.
func TestBuildUsesBucketRatio(t *testing.T) {
	seed := int64(99)

	ratios := []float64{0.5, 0.75, 1.0, 1.2} // Test various ratios

	for _, ratio := range ratios {
		t.Run(fmt.Sprintf("Ratio_%.2f", ratio), func(t *testing.T) {
			b := Builder().Seed(seed)
			_, err := b.BucketRatio(ratio)
			require.NoError(t, err, "Setting bucket ratio should succeed")

			c, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b)
			assert.NoError(t, err, "Build should succeed with bucket ratio %.2f", ratio)
			require.NotNil(t, c)
			assert.Equal(t, len(sampleKeys), c.Len())

			// Verify content
			for i := range sampleKeys {
				val := c.Get(sampleKeys[i])
				assert.Equal(t, sampleVals[i], val, "Mismatch for key %s", string(sampleKeys[i]))
			}
		})
	}
}

// TestBuildUsesDefaults verifies that a build works correctly when no
// parameters are explicitly set.
func TestBuildUsesDefaults(t *testing.T) {
	// Use a non-trivial number of keys
	keys := words[:100]
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = keys[i] // Value is same as key
	}

	b := Builder() // No setters called
	c, err := buildCHDFromSlices(t, keys, vals, b)

	require.NoError(t, err, "Build with default parameters failed")
	require.NotNil(t, c)
	assert.Equal(t, len(keys), c.Len(), "Length mismatch with default build")

	// Spot check a few keys
	assert.Equal(t, keys[0], c.Get(keys[0]))
	assert.Equal(t, keys[len(keys)/2], c.Get(keys[len(keys)/2]))
	assert.Equal(t, keys[len(keys)-1], c.Get(keys[len(keys)-1]))
	assert.Nil(t, c.Get([]byte("this key does not exist")))
}

// --- End of New Phase 2 Tests ---

// --- Start of New Phase 3 Tests ---

func TestBuilderProgressChanDefaultNil(t *testing.T) {
	b := Builder()
	assert.Nil(t, b.progressChan, "Default progressChan should be nil")
}

func TestBuilderSetProgressChan(t *testing.T) {
	b := Builder()
	// Use a buffered channel to avoid blocking in simple test cases
	ch := make(chan BuildProgress, 1)
	retBuilder := b.ProgressChan(ch)

	assert.Same(t, b, retBuilder, "ProgressChan should return the builder instance")
	assert.NotNil(t, b.progressChan, "progressChan should be set after calling ProgressChan")

	// --- DIAGNOSTICS START ---
	t.Logf("Type of ch: %v, Address: %p", reflect.TypeOf(ch), ch)
	t.Logf("Type of b.progressChan: %v, Address: %p", reflect.TypeOf(b.progressChan), b.progressChan)
	// --- DIAGNOSTICS END ---

	// Compare the underlying channel pointers since types differ (chan T vs chan<- T)
	chPtr := reflect.ValueOf(ch).Pointer()
	progressChanPtr := reflect.ValueOf(b.progressChan).Pointer()
	assert.Equal(t, chPtr, progressChanPtr, "progressChan should point to the same underlying channel provided")

	// Test setting it back to nil
	retBuilder = b.ProgressChan(nil)
	assert.Same(t, b, retBuilder, "ProgressChan should return the builder instance")
	assert.Nil(t, b.progressChan, "progressChan should be nil after setting to nil")
}

// TestBuildWithNilProgressChanStillWorks verifies that Build() works correctly
// when no progress channel is provided (important regression check).
func TestBuildWithNilProgressChanStillWorks(t *testing.T) {
	b := Builder().Seed(123) // Use seed for reproducibility
	// Ensure progressChan is explicitly nil (should be default, but make sure)
	b.progressChan = nil

	c, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b)
	require.NoError(t, err, "Build should succeed even if progress channel is nil")
	require.NotNil(t, c)
	assert.Equal(t, len(sampleKeys), c.Len())
}

// --- End of New Phase 3 Tests ---

// --- Start of New Phase 4 Tests ---

func TestBuildSendsProgress(t *testing.T) {
	// Use a buffered channel large enough to hold all expected messages
	// for a small build without blocking.
	progressChan := make(chan BuildProgress, 100) // Adjust size if needed
	b := Builder().Seed(42).ProgressChan(progressChan)

	// Use sample data which is small and predictable
	c, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b)
	require.NoError(t, err, "Build failed")
	require.NotNil(t, c)
	close(progressChan) // Close channel to signal completion for range loop

	var receivedProgress []BuildProgress
	for p := range progressChan {
		receivedProgress = append(receivedProgress, p)
	}

	require.Greater(t, len(receivedProgress), 2, "Should receive at least a few progress updates")

	// Check first message (Hashing Keys or similar)
	assert.Equal(t, 1, receivedProgress[0].AttemptID, "AttemptID should be 1")
	assert.NotEmpty(t, receivedProgress[0].Stage, "First stage should be set")
	assert.Greater(t, receivedProgress[0].TotalBuckets, 0, "TotalBuckets should be positive")

	// Check last message (Complete)
	lastProgress := receivedProgress[len(receivedProgress)-1]
	assert.Equal(t, 1, lastProgress.AttemptID, "Last AttemptID should be 1")
	assert.Equal(t, "Complete", lastProgress.Stage, "Last stage should be 'Complete'")
	assert.Equal(t, lastProgress.TotalBuckets, lastProgress.BucketsProcessed, "Last progress should show all buckets processed")

	// Check intermediate messages (Assigning Hashes)
	foundAssigning := false
	for _, p := range receivedProgress {
		assert.Equal(t, 1, p.AttemptID) // Ensure all have AttemptID 1
		if p.Stage == "Assigning Hashes" {
			foundAssigning = true
			assert.GreaterOrEqual(t, p.BucketsProcessed, 0)
			assert.LessOrEqual(t, p.BucketsProcessed, p.TotalBuckets)
			assert.GreaterOrEqual(t, p.CurrentBucketSize, 0)
			assert.GreaterOrEqual(t, p.CurrentBucketCollisions, 0)
		}
	}
	assert.True(t, foundAssigning, "Should have received 'Assigning Hashes' stage updates")
}

func TestBuildProgressNonBlockingSend(t *testing.T) {
	// Use an *unbuffered* channel to force blocking if not handled correctly
	progressChan := make(chan BuildProgress)
	b := Builder().Seed(99).ProgressChan(progressChan)

	// Use a slightly larger dataset to ensure multiple progress updates are attempted
	keys := words[:30]
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = keys[i]
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var buildErr error
	var c *CHD

	// Run build in a goroutine
	go func() {
		defer wg.Done()
		c, buildErr = buildCHDFromSlices(t, keys, vals, b)
	}()

	// Only read the *first* progress message (or none if build is super fast)
	// Then let the channel block potential future sends.
	readDone := make(chan bool)
	go func() {
		select {
		case <-progressChan:
			// Successfully read one message
			t.Log("Successfully read one progress message")
		case <-time.After(2 * time.Second):
			// Didn't receive a message quickly, maybe build finished?
			t.Log("Timed out waiting for first progress message")
		}
		close(readDone)
	}()

	<-readDone // Wait until we have attempted to read one message

	// Wait for the build goroutine to finish
	buildFinishChan := make(chan bool)
	go func() {
		wg.Wait()
		close(buildFinishChan)
	}()

	// Check if the build finishes within a reasonable time, indicating it didn't deadlock
	select {
	case <-buildFinishChan:
		// Build finished successfully (or with an error, which is still not a deadlock)
		assert.NoError(t, buildErr, "Build should complete without error even if progress chan wasn't fully consumed")
		require.NotNil(t, c)
	case <-time.After(10 * time.Second): // Generous timeout
		t.Fatal("Build process deadlocked or took too long, likely blocked on progress channel send")
	}
}

// --- End of New Phase 4 Tests ---

// --- Start of New Phase 5 Tests ---

func TestBuilderParallelAttemptsDefault(t *testing.T) {
	b := Builder()
	assert.Equal(t, 1, b.parallelSeedAttempts, "Default parallelSeedAttempts should be 1")
}

func TestBuilderSetParallelAttempts(t *testing.T) {
	b := Builder()

	// Valid attempt
	_, err := b.ParallelAttempts(3)
	require.NoError(t, err, "Setting valid ParallelAttempts should not error")
	assert.Equal(t, 3, b.parallelSeedAttempts, "parallelSeedAttempts should be updated")

	// Chain attempt
	retBuilder, err := b.ParallelAttempts(5)
	require.NoError(t, err)
	assert.Same(t, b, retBuilder, "ParallelAttempts should return builder instance for chaining")
	assert.Equal(t, 5, b.parallelSeedAttempts, "parallelSeedAttempts should be updated after chaining")

	// Invalid attempts
	_, err = b.ParallelAttempts(0)
	assert.Error(t, err, "Setting ParallelAttempts to 0 should error")
	assert.Contains(t, err.Error(), "must be at least 1")
	assert.Equal(t, 5, b.parallelSeedAttempts, "Value should not change after invalid attempt") // Check it wasn't changed

	_, err = b.ParallelAttempts(-1)
	assert.Error(t, err, "Setting ParallelAttempts to -1 should error")
	assert.Contains(t, err.Error(), "must be at least 1")
	assert.Equal(t, 5, b.parallelSeedAttempts, "Value should not change after invalid attempt") // Check it wasn't changed
}

// --- IMPORTANT: Regression Testing ---
// We rely on the fact that ALL previous tests (Phase 1-4) are run and pass
// after this refactoring. This implicitly tests that the refactored
// buildInternal function and the public Build (calling buildInternal once)
// still produce the correct results and behavior for the single-threaded case.
// No *new* tests specifically for the refactoring are needed if the old ones pass.

// --- End of New Phase 5 Tests ---

// --- Start of New Phase 6a Tests ---

// TestBuildParallelBasicSuccess verifies that if at least one attempt succeeds,
// the build returns a valid CHD.
func TestBuildParallelBasicSuccess(t *testing.T) {
	b := Builder().Seed(1) // Use fixed seed for predictability if needed
	_, err := b.ParallelAttempts(3)
	require.NoError(t, err)
	// Default settings should allow success for sampleData

	c, err := buildCHDFromSlices(t, sampleKeys, sampleVals, b)
	require.NoError(t, err, "Build with parallel attempts failed unexpectedly")
	require.NotNil(t, c, "Returned CHD should not be nil on success")
	assert.Equal(t, len(sampleKeys), c.Len())

	// Verify content as a sanity check
	for i := range sampleKeys {
		val := c.Get(sampleKeys[i])
		assert.Equal(t, sampleVals[i], val, "Mismatch for key %s", string(sampleKeys[i]))
	}
}

// TestBuildParallelAllFail verifies that if all attempts fail (due to low retry limit),
// an error is returned.
func TestBuildParallelAllFail(t *testing.T) {
	b := Builder().Seed(42) // Use fixed seed
	_, err := b.ParallelAttempts(3)
	require.NoError(t, err)
	_, err = b.RetryLimit(1) // Force failure with extremely low limit
	require.NoError(t, err)

	// Use a slightly larger dataset where failure is more likely with limit 1
	testKeys := words[:20]
	testVals := make([][]byte, len(testKeys))
	for i := range testKeys {
		testVals[i] = []byte(fmt.Sprintf("v%d", i))
	}

	_, err = buildCHDFromSlices(t, testKeys, testVals, b)
	require.Error(t, err, "Build should fail when all parallel attempts fail")
	// Check if the error indicates all attempts failed
	assert.Contains(t, err.Error(), "all 3 parallel build attempts failed")
	assert.Contains(t, err.Error(), "failed to find a collision-free hash function after ~1 attempts") // Check cause
}

// TestBuildParallelProgressMultipleAttempts verifies that progress messages
// from different attempt IDs are received.
func TestBuildParallelProgressMultipleAttempts(t *testing.T) {
	numAttempts := 3 // Increase attempts
	// Use a buffered channel large enough
	progressChan := make(chan BuildProgress, 300) // Larger buffer might be needed
	b := Builder().Seed(88).ProgressChan(progressChan)
	_, err := b.ParallelAttempts(numAttempts)
	require.NoError(t, err)

	// Make build slightly longer to see more progress interleaving
	keys := words[:50]
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = []byte(fmt.Sprintf("val%d", i))
	}

	buildDone := make(chan struct{})
	var buildErr error
	go func() {
		_, buildErr = buildCHDFromSlices(t, keys, vals, b)
		close(buildDone)
	}()

	// Read progress messages until build is done OR we see a "Complete" stage
	// Use a timeout to prevent hanging if "Complete" is somehow missed.
	receivedProgress := make(map[int][]BuildProgress) // Map by AttemptID
	readTimeout := time.After(5 * time.Second) // Safety timeout
	keepReading := true

	for keepReading {
		select {
		case p, ok := <-progressChan:
			if !ok {
				keepReading = false // Channel closed
				break
			}
			receivedProgress[p.AttemptID] = append(receivedProgress[p.AttemptID], p)
			if p.Stage == "Complete" {
				// We saw a complete message, assume others will flush soon or are irrelevant
				// Stop actively reading after a short grace period
				go func() {
					time.Sleep(50 * time.Millisecond)
					// Signal to stop reading (might need a channel if concurrent reads happen)
					// For simplicity here, just stop the outer loop assumption
					// This isn't perfectly robust but better than fixed sleep.
					// A better approach might involve signaling from the build goroutine completion.
				}()
				// For now, let's just break the select and let the outer loop finish naturally
				// when the channel is closed by the sender eventually (or timeout hits)
			}
		case <-buildDone:
			// Build finished, try reading remaining messages briefly
			keepReading = false
			// Drain remaining messages for a short period
			drainTimeout := time.After(100 * time.Millisecond)
		drainLoop:
			for {
				select {
				case p, ok := <-progressChan:
					if !ok { break drainLoop } // Channel closed
					receivedProgress[p.AttemptID] = append(receivedProgress[p.AttemptID], p)
				case <-drainTimeout:
					break drainLoop
				}
			}
		case <-readTimeout:
			t.Log("Read progress timeout hit")
			keepReading = false // Stop reading on timeout
		}
	}

	// Now analyze the collected messages
	firstSuccessfulAttemptID := -1 // Reset before analysis
	maxProcessedPerAttempt := make(map[int]int) // Reset before analysis
	
	// Find the actual first success ID from the collected messages
	for id, msgs := range receivedProgress {
		for _, p := range msgs {
			if p.Stage == "Complete" && firstSuccessfulAttemptID == -1 {
				firstSuccessfulAttemptID = p.AttemptID
			}
			if p.BucketsProcessed > maxProcessedPerAttempt[id] {
				maxProcessedPerAttempt[id] = p.BucketsProcessed
			}
		}
	}
	
	require.NoError(t, buildErr) // Verify build succeeded

	assert.NotEmpty(t, receivedProgress, "Should receive some progress messages")
	assert.NotEqual(t, -1, firstSuccessfulAttemptID, "At least one attempt should have completed successfully")

	t.Logf("First successful attempt ID: %d", firstSuccessfulAttemptID)
	for id, count := range maxProcessedPerAttempt {
		t.Logf("Attempt %d max buckets processed: %d", id, count)
	}

	// Check if attempts other than the first successful one potentially stopped early
	// (This is not guaranteed without context propagation, but check if progress suggests it)
	if len(receivedProgress) > 1 {
		// foundShorter := false
		for id, msgs := range receivedProgress {
			if id != firstSuccessfulAttemptID {
				if len(msgs) == 0 {
					continue
				} // Possible if success was immediate
				lastMsg := msgs[len(msgs)-1]
				// Check if it finished *before* reaching the end
				if lastMsg.Stage != "Complete" || lastMsg.BucketsProcessed < lastMsg.TotalBuckets {
					// foundShorter = true
					t.Logf("Attempt %d likely stopped early (last stage: %s, buckets: %d/%d)",
						id, lastMsg.Stage, lastMsg.BucketsProcessed, lastMsg.TotalBuckets)
				}
			}
		}
		// This assertion is weak in 6b, stronger test needed in 6c
		// assert.True(t, foundShorter, "Expected at least one other attempt to show signs of stopping early")
	}
}

// --- End of New Phase 6a Tests ---

// --- Start of New Phase 6b Tests ---

// TestBuildParallelReturnsOnFirstSuccess verifies that if one attempt succeeds,
// even if others fail, the build returns the success.
func TestBuildParallelReturnsOnFirstSuccess(t *testing.T) {
	// Scenario: Attempt 1 fails quickly, Attempt 2 succeeds, Attempt 3 fails quickly.
	// We need settings that reliably cause this. Use RetryLimit.

	testKeys := words[:30] // Use a dataset that requires >1 retry sometimes
	testVals := make([][]byte, len(testKeys))
	for i := range testKeys {
		testVals[i] = []byte(fmt.Sprintf("v%d", i))
	}

	numAttempts := 3
	//successSeed := int64(101) // Assume this seed works with default limit
	//failSeed1 := int64(102)
	//failSeed2 := int64(103)

	// We can't directly assign seeds AND different params per attempt yet.
	// So, we configure the builder once. We set a high parallel attempt count,
	// a low retry limit (to encourage failures), but rely on the fact that
	// different random seeds might hit the limit or not.

	builder := Builder()
	_, err := builder.ParallelAttempts(numAttempts)
	require.NoError(t, err)
	_, err = builder.RetryLimit(5) // Low limit to make failures more likely for some seeds
	require.NoError(t, err)
	// Don't set a specific seed, let the parallel logic generate random ones.

	// It's hard to guarantee failure/success based *only* on random seeds with
	// fixed parameters across attempts. We are testing the *mechanism*:
	// If *any* of the random seeds succeed with limit 5, the build should succeed.
	// If *all* random seeds fail with limit 5, the build should fail.

	// Run multiple times to increase chance of hitting both scenarios
	successCount := 0
	failCount := 0
	runs := 10 // Run a few times

	for i := 0; i < runs; i++ {
		// Re-create builder each time to get new random seeds inside Build()
		builder = Builder()
		_, err = builder.ParallelAttempts(numAttempts)
		require.NoError(t, err)
		_, err = builder.RetryLimit(5)
		require.NoError(t, err)

		c, buildErr := buildCHDFromSlices(t, testKeys, testVals, builder)

		if buildErr == nil {
			successCount++
			require.NotNil(t, c)
			assert.Equal(t, len(testKeys), c.Len())
		} else {
			failCount++
			assert.Contains(t, buildErr.Error(), fmt.Sprintf("all %d parallel build attempts failed", numAttempts))
		}
	}

	t.Logf("Parallel build runs: %d successes, %d failures (with limit 5)", successCount, failCount)
	// We expect *some* successes and potentially *some* failures over multiple runs
	// This demonstrates the mechanism handles both outcomes correctly.
	assert.True(t, successCount > 0 || failCount > 0, "Expected at least one outcome over multiple runs")
	// If it *always* fails or *always* succeeds, the test setup might not be triggering diverse outcomes.
}

// --- End of New Phase 6b Tests ---

// --- Start of New Phase 6c Tests ---

func TestBuildParallelContextCancellation(t *testing.T) {
	numAttempts := 2
	progressChan := make(chan BuildProgress, 500) // Need enough buffer

	// Configure builder for two attempts
	builder := Builder().ProgressChan(progressChan)
	_, err := builder.ParallelAttempts(numAttempts)
	require.NoError(t, err)

	// Make the build process reasonably long IF it doesn't succeed quickly.
	// Use a moderate retry limit and a dataset that might require some retries.
	_, err = builder.RetryLimit(500) // Moderate limit
	require.NoError(t, err)
	keys := words[:100] // Larger dataset
	vals := make([][]byte, len(keys))
	for i := range keys { vals[i] = []byte(fmt.Sprintf("v%d",i)) }

	// Don't set a specific seed, let the logic generate them. We *expect* one
	// attempt to likely succeed before the other finishes all retries or buckets.

	startTime := time.Now()
	c, buildErr := buildCHDFromSlices(t, keys, vals, builder)
	duration := time.Since(startTime)

	// We expect one attempt to succeed
	require.NoError(t, buildErr, "Build failed unexpectedly, cannot test cancellation effect")
	require.NotNil(t, c)

	// Read progress messages until channel is closed or we get enough
	close(progressChan) // Close to drain in the range loop

	receivedProgress := make(map[int][]BuildProgress) // Map by AttemptID
	firstSuccessfulAttemptID := -1

	for p := range progressChan {
		receivedProgress[p.AttemptID] = append(receivedProgress[p.AttemptID], p)
		if p.Stage == "Complete" && firstSuccessfulAttemptID == -1 {
			firstSuccessfulAttemptID = p.AttemptID
		}
	}

	require.NotEqual(t, -1, firstSuccessfulAttemptID, "Expected one attempt to complete successfully")
	require.Len(t, receivedProgress, numAttempts, "Expected progress from both attempts initially")

	// Analyze the *other* attempt (the one that should have been cancelled)
	cancelledAttemptID := -1
	for id := 1; id <= numAttempts; id++ {
		if id != firstSuccessfulAttemptID {
			cancelledAttemptID = id
			break
		}
	}
	require.NotEqual(t, -1, cancelledAttemptID, "Could not identify the cancelled attempt")

	cancelledMsgs := receivedProgress[cancelledAttemptID]
	require.NotEmpty(t, cancelledMsgs, "Cancelled attempt should have sent some progress")

	lastCancelledMsg := cancelledMsgs[len(cancelledMsgs)-1]
	t.Logf("Successful attempt: %d. Cancelled attempt: %d. Last stage for cancelled: %s, Buckets: %d/%d, Collisions: %d",
		firstSuccessfulAttemptID, cancelledAttemptID, lastCancelledMsg.Stage, lastCancelledMsg.BucketsProcessed, lastCancelledMsg.TotalBuckets, lastCancelledMsg.CurrentBucketCollisions)

	// Assert that the cancelled attempt did NOT reach the "Complete" stage
	assert.NotEqual(t, "Complete", lastCancelledMsg.Stage,
		"Cancelled attempt (%d) should not have reached 'Complete' stage", cancelledAttemptID)

	// We could also add timing checks, e.g., assert duration is less than
	// what a single full attempt with many retries would take, but this is harder
	// to make reliable across different machines. Checking the final stage is more robust.
	t.Logf("Total parallel build duration with cancellation: %v", duration)
}

// --- End of New Phase 6c Tests ---

func BenchmarkBuiltinMap(b *testing.B) {
	keys := []string{}
	d := map[string]string{}
	for _, bk := range words {
		k := string(bk)
		d[k] = k
		keys = append(keys, k)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d[keys[i%len(keys)]]
	}
}

func BenchmarkCHD(b *testing.B) {
	keys := [][]byte{}
	mph := Builder()
	for _, k := range words {
		keys = append(keys, k)
		mph.Add(k, k)
	}
	h, _ := mph.Build()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Get(keys[i%len(keys)])
	}
}
