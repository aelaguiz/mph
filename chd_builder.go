package mph

import (
	"bytes"
	"context" // Added for cancellation
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type chdHasher struct {
	r       []uint64
	size    uint64
	buckets uint64
	rand    *rand.Rand
}

type bucket struct {
	index  uint64
	keys   [][]byte
	values [][]byte
}

func (b *bucket) String() string {
	a := "bucket{"
	for _, k := range b.keys {
		a += string(k) + ", "
	}
	return a + "}"
}

// Intermediate data structure storing buckets + outer hash index.
type bucketVector []bucket

func (b bucketVector) Len() int           { return len(b) }
func (b bucketVector) Less(i, j int) bool { return len(b[i].keys) > len(b[j].keys) }
func (b bucketVector) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// Build a new CDH MPH.
type CHDBuilder struct {
	keys   [][]byte
	values [][]byte

	// User-configurable parameters
	bucketRatio   float64 // Ratio of initial buckets to keys (m/n). Default: 0.5
	retryLimit    int     // Max attempts to find a collision-free hash for a bucket. Default: 10,000,000
	userSeed      int64   // Seed provided by the user via Seed().
	seedSetByUser bool    // Flag indicating if Seed() was called.

	// Parallel execution control
	parallelSeedAttempts int // Number of different seeds to try in parallel if the first fails. Default: 1

	// Progress reporting
	progressChan chan<- BuildProgress // Optional channel for progress updates.
}

// BuildProgress provides information about the state of the build process.
type BuildProgress struct {
	AttemptID               int    // Identifier for the parallel attempt (starts at 1).
	BucketsProcessed        int    // Number of buckets whose hash functions have been assigned.
	TotalBuckets            int    // Total number of initial buckets to process.
	CurrentBucketSize       int    // Number of keys in the bucket currently being processed.
	CurrentBucketCollisions int    // Number of hash functions tried for the current bucket so far.
	Stage                   string // Description of the current build stage (e.g., "Hashing Keys", "Sorting Buckets", "Assigning Hashes")
}

// Create a new CHD hash table builder.
// Defaults: BucketRatio=0.5, RetryLimit=10,000,000, Seed=time-based.
func Builder() *CHDBuilder {
	return &CHDBuilder{
		// Default values
		bucketRatio: 0.5,
		retryLimit:  10_000_000,
		// userSeed defaults to 0
		// seedSetByUser defaults to false
		parallelSeedAttempts: 1, // Default to single attempt
		// progressChan defaults to nil
	}
}

// Seed the internal random number generator.
// Calling this ensures reproducible builds for the same set of keys added in the same order.
// If not called, a time-based seed is used.
func (b *CHDBuilder) Seed(seed int64) *CHDBuilder {
	b.userSeed = seed
	b.seedSetByUser = true
	return b // Return builder for chaining
}

// Add a key and value to the hash table.
func (b *CHDBuilder) Add(key []byte, value []byte) {
	b.keys = append(b.keys, key)
	b.values = append(b.values, value)
}

// BucketRatio sets the ratio of initial hash buckets to the number of keys (m/n).
// A higher ratio uses more memory for the intermediate bucket index but might
// reduce the number of collisions during the assignment phase.
// The ratio must be greater than 0.0. Common values are 0.5 or 1.0.
// Default: 0.5
func (b *CHDBuilder) BucketRatio(ratio float64) (*CHDBuilder, error) {
	if ratio <= 0.0 {
		return nil, fmt.Errorf("bucket ratio must be greater than 0.0, got %f", ratio)
	}
	b.bucketRatio = ratio
	return b, nil // Return builder for chaining
}

// RetryLimit sets the maximum number of attempts to find a collision-free
// secondary hash function ('r' value) for the keys within a single bucket.
// If this limit is exceeded for any bucket, the build fails.
// Default: 10,000,000
func (b *CHDBuilder) RetryLimit(limit int) (*CHDBuilder, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("retry limit must be greater than 0, got %d", limit)
	}
	b.retryLimit = limit
	return b, nil // Return builder for chaining
}

// ParallelAttempts sets the number of different random seeds the builder
// should attempt in parallel during the Build() process.
// If set > 1, Build() will launch multiple concurrent build attempts. The first
// one to succeed returns its result, and the others are cancelled.
// Default: 1 (single attempt). Must be >= 1.
func (b *CHDBuilder) ParallelAttempts(n int) (*CHDBuilder, error) {
	if n < 1 {
		return nil, fmt.Errorf("parallel attempts must be at least 1, got %d", n)
	}
	b.parallelSeedAttempts = n
	return b, nil
}

// ProgressChan sets an optional channel for receiving BuildProgress updates.
// The channel should be buffered or consumed quickly to avoid blocking the build.
func (b *CHDBuilder) ProgressChan(ch chan<- BuildProgress) *CHDBuilder {
	b.progressChan = ch
	return b // Return builder for chaining
}

// sendProgress sends a progress update non-blockingly if the channel is configured.
// It uses the provided attemptID in the progress update.
func (b *CHDBuilder) sendProgress(progress BuildProgress, attemptID int) {
	// Ensure AttemptID is set correctly
	if progress.AttemptID == 0 {
		progress.AttemptID = attemptID
	}

	if b.progressChan != nil {
		select {
		case b.progressChan <- progress:
			// Sent successfully
		default:
			// Channel buffer is full or receiver is not ready; drop progress update.
		}
	}
}

// Try to find a hash function that does not cause collisions with table, when
// applied to the keys in the bucket.
func tryHash(hasher *chdHasher, seen map[uint64]bool, keys [][]byte, values [][]byte, indices []uint16, bucket *bucket, ri uint16, r uint64) bool {
	// Track duplicates within this bucket.
	duplicate := make(map[uint64]bool)
	// Make hashes for each entry in the bucket.
	hashes := make([]uint64, len(bucket.keys))
	for i, k := range bucket.keys {
		h := hasher.Table(r, k)
		hashes[i] = h
		if seen[h] {
			return false
		}
		if duplicate[h] {
			return false
		}
		duplicate[h] = true
	}

	// Update seen hashes
	for _, h := range hashes {
		seen[h] = true
	}

	// Add the hash index.
	indices[bucket.index] = ri

	// Update the the hash table.
	for i, h := range hashes {
		keys[h] = bucket.keys[i]
		values[h] = bucket.values[i]
	}
	return true
}

// Internal struct to hold results from parallel build attempts
type buildResult struct {
	chd *CHD
	err error
}

// Build constructs the minimal perfect hash table.
// It may launch multiple attempts in parallel if ParallelAttempts > 1.
func (b *CHDBuilder) Build() (*CHD, error) {
	// Phase 5: Just call the internal build function once.
	// Phase 6 will add the parallel logic here.

	// Determine the initial seed for the first (and currently only) attempt
	initialSeed := b.userSeed
	if !b.seedSetByUser {
		initialSeed = time.Now().UnixNano()
	}

	// --- Single Attempt Path ---
	if b.parallelSeedAttempts == 1 {
		return b.buildInternal(context.Background(), initialSeed, 1) // Attempt ID 1
	}

	// --- Parallel Attempt Path (Phase 6b Logic) ---
	numAttempts := b.parallelSeedAttempts
	resultsChan := make(chan buildResult, numAttempts) // Buffered channel for all results

	// Use context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled eventually

	// Generate distinct seeds for each attempt
	attemptSeeds := make([]int64, numAttempts)
	// Use a local RNG source for generating attempt seeds so it doesn't interfere
	// with the builder's potential time-based seed or the seeds used *inside* buildInternal.
	seedGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	firstSeed := initialSeed
	attemptSeeds[0] = firstSeed
	for i := 1; i < numAttempts; i++ {
		// Generate random seeds, ensuring they differ from the first one
		// (duplicates among random seeds are unlikely and acceptable)
		seed := seedGen.Int63()
		if b.seedSetByUser && seed == firstSeed { // Avoid reusing user seed if possible
			seed = seedGen.Int63()
		}
		attemptSeeds[i] = seed
	}

	// Launch goroutines
	for i := 0; i < numAttempts; i++ {
		go func(attemptIdx int) {
			seed := attemptSeeds[attemptIdx]
			attemptID := attemptIdx + 1 // 1-based ID

			// Pass the context to the internal build function
			chdResult, errResult := b.buildInternal(ctx, seed, attemptID)

			// Send result regardless of context cancellation state for now.
			// The receiver loop will handle ignoring late results.
			resultsChan <- buildResult{chd: chdResult, err: errResult}
		}(i)
	}

	// Wait for the first success or all errors
	var firstSuccess *CHD
	var lastError error
	errorCount := 0

	for i := 0; i < numAttempts; i++ { // Loop exactly numAttempts times to collect all results
		select {
		case result := <-resultsChan:
			if result.err == nil && result.chd != nil {
				// First success!
				if firstSuccess == nil {
					firstSuccess = result.chd
					cancel() // Signal other goroutines to stop (they might not react yet in 6b)
					// Note: We still loop numAttempts times to drain the channel,
					// but we've captured the first success.
				}
				// Ignore subsequent successes
			} else if result.err != nil {
				// Received an error
				errorCount++
				lastError = result.err // Keep track of the latest error
			}
			// Handle case where result.chd is nil and result.err is nil (shouldn't happen from buildInternal)
			// Optional: Add a timeout case here if desired using time.After
			// case <-time.After(someTimeout):
			//     cancel()
			//     return nil, errors.New("build timed out")
		}
	}

	if firstSuccess != nil {
		return firstSuccess, nil // Return the first success we found
	}

	// If no success was found
	if lastError != nil {
		return nil, fmt.Errorf("all %d parallel build attempts failed, last error: %w", numAttempts, lastError)
	}

	// Should be rare: no success and no error (e.g., empty input?)
	return nil, errors.New("all parallel build attempts completed without success or error")
}

// buildInternal performs a single attempt to build the CHD table using a specific seed.
// buildInternal performs a single attempt to build the CHD table using a specific seed.
// It checks the provided context for cancellation requests.
func (b *CHDBuilder) buildInternal(ctx context.Context, buildSeed int64, attemptID int) (*CHD, error) {
	// Check for cancellation at the very beginning
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	n := uint64(len(b.keys))
	mFloat := float64(n) * b.bucketRatio
	m := uint64(mFloat)
	if m == 0 {
		m = 1
	}
	totalBuckets := int(m) // Store for progress reporting

	keys := make([][]byte, n)
	values := make([][]byte, n)
	// Use the specific seed provided for this attempt
	hasher := newCHDHasher(n, m, buildSeed, true) // Treat the provided seed as explicitly set for this attempt
	buckets := make(bucketVector, m)

	b.sendProgress(BuildProgress{
		TotalBuckets: totalBuckets,
		Stage:        "Hashing Keys",
	}, attemptID)

	// --- Hashing Keys Stage ---
	indices := make([]uint16, m)
	// An extra check to make sure we don't use an invalid index
	for i := range indices {
		indices[i] = ^uint16(0)
	}
	// Have we seen a hash before?
	seen := make(map[uint64]bool)
	// Used to ensure there are no duplicate keys.
	duplicates := make(map[string]bool)

	for i := range b.keys {
		key := b.keys[i]
		value := b.values[i]
		k := string(key)
		if duplicates[k] {
			return nil, errors.New("duplicate key " + k)
		}
		duplicates[k] = true
		oh := hasher.HashIndexFromKey(key)

		buckets[oh].index = oh
		buckets[oh].keys = append(buckets[oh].keys, key)
		buckets[oh].values = append(buckets[oh].values, value)
	}

	b.sendProgress(BuildProgress{
		TotalBuckets: totalBuckets,
		Stage:        "Sorting Buckets",
	}, attemptID)

	// --- Sorting Buckets Stage ---
	// Order buckets by size (retaining the hash index)
	collisions := 0
	sort.Sort(buckets)

	// --- Assigning Hashes Stage ---
	// Send initial assignment state
	b.sendProgress(BuildProgress{
		TotalBuckets: totalBuckets,
		Stage:        "Assigning Hashes",
	}, attemptID)
	
	// Check for cancellation before starting main loop
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

nextBucket:
	for i, bucket := range buckets {
		if len(bucket.keys) == 0 {
			continue
		}
		
		// Check for cancellation at the start of processing each bucket
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		b.sendProgress(BuildProgress{
			BucketsProcessed:  i, // Report progress *before* processing bucket i
			TotalBuckets:      totalBuckets,
			CurrentBucketSize: len(bucket.keys),
			Stage:             "Assigning Hashes",
		}, attemptID)

		// Check existing hash functions.
		for ri, r := range hasher.r {
			if tryHash(hasher, seen, keys, values, indices, &bucket, uint16(ri), r) {
				continue nextBucket
			}
		}

		// Keep trying new functions until we get one that does not collide.
		// The number of retries here is very high to allow a very high
		// probability of not getting collisions.
		// Check for cancellation before entering potentially long retry loop
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		
		for coll := 0; coll < b.retryLimit; coll++ {
			b.sendProgress(BuildProgress{
				BucketsProcessed:        i,
				TotalBuckets:            totalBuckets,
				CurrentBucketSize:       len(bucket.keys),
				CurrentBucketCollisions: coll + 1, // Report attempts (1-based)
				Stage:                   "Assigning Hashes",
			}, attemptID)

			if coll > collisions {
				collisions = coll
			}
			ri, r := hasher.Generate()
			if tryHash(hasher, seen, keys, values, indices, &bucket, ri, r) {
				hasher.Add(r)
				continue nextBucket
			}
		}

		// Failed to find a hash function with no collisions.
		return nil, fmt.Errorf(
			"failed to find a collision-free hash function after ~%d attempts, for bucket %d/%d with %d entries: %s",
			b.retryLimit, i, totalBuckets, len(bucket.keys), &bucket)
	}

	// println("max bucket collisions:", collisions)
	// println("keys:", len(table))
	// println("hash functions:", len(hasher.r))

	// Check for cancellation before packing data
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	b.sendProgress(BuildProgress{
		BucketsProcessed: totalBuckets, // All buckets processed
		TotalBuckets:     totalBuckets,
		Stage:            "Packing Data",
	}, attemptID)

	keylist := make([]dataSlice, len(b.keys))
	valuelist := make([]dataSlice, len(b.values))
	var buf bytes.Buffer
	for i, k := range keys {
		keylist[i].start = uint64(buf.Len())
		buf.Write(k)
		keylist[i].end = uint64(buf.Len())
		valuelist[i].start = uint64(buf.Len())
		buf.Write(values[i])
		valuelist[i].end = uint64(buf.Len())
	}

	// If we got here, packing is done. Send Complete and return success.
	// Do NOT check for cancellation here, otherwise the winning goroutine might fail to report success.

	// Diagnostic logging to confirm "Complete" message is being sent
	fmt.Printf("DEBUG: Sending 'Complete' message for attempt ID %d\n", attemptID)
	b.sendProgress(BuildProgress{
		BucketsProcessed: totalBuckets,
		TotalBuckets:     totalBuckets,
		Stage:            "Complete", // Final status
	}, attemptID)
	fmt.Printf("DEBUG: Sent 'Complete' message for attempt ID %d\n", attemptID)

	return &CHD{
		r:       hasher.r,
		indices: indices,
		mmap:    buf.Bytes(),
		keys:    keylist,
		values:  valuelist,
	}, nil
}

func newCHDHasher(size, buckets uint64, seed int64, seeded bool) *chdHasher {
	if !seeded {
		seed = time.Now().UnixNano()
	}
	rs := rand.NewSource(seed)
	c := &chdHasher{size: size, buckets: buckets, rand: rand.New(rs)}
	c.Add(c.rand.Uint64())
	return c
}

// Hash index from key.
func (h *chdHasher) HashIndexFromKey(b []byte) uint64 {
	return (hasher(b) ^ h.r[0]) % h.buckets
}

// Table hash from random value and key. Generate() returns these random values.
func (h *chdHasher) Table(r uint64, b []byte) uint64 {
	return (hasher(b) ^ h.r[0] ^ r) % h.size
}

func (c *chdHasher) Generate() (uint16, uint64) {
	return c.Len(), c.rand.Uint64()
}

// Add a random value generated by Generate().
func (c *chdHasher) Add(r uint64) {
	c.r = append(c.r, r)
}

func (c *chdHasher) Len() uint16 {
	return uint16(len(c.r))
}

func (h *chdHasher) String() string {
	return fmt.Sprintf("chdHasher{size: %d, buckets: %d, r: %v}", h.size, h.buckets, h.r)
}
