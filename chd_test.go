package mph

import (
	"bufio"
	"bytes"
	"io"
	"os"
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
)

var (
	words [][]byte
)

func init() {
	f, err := os.Open("testdata/words")
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		words = append(words, line)
	}
}

func cmpDataSlice(t *testing.T, cl, cr *CHD, l, r []dataSlice) {
	t.Helper()
	assert.Equal(t, len(l), len(r))
	for i := 0; len(l) > i; i++ {
		assert.Equal(t, cl.slice(l[i]), cr.slice(r[i]))
	}
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
