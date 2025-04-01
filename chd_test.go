package mph

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect" // Added for diagnostics
	"io"
	"os"
	"runtime"
	"testing"

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
