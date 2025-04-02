package mph

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to generate keys with sequential prefixes
func generateSequentialKeys(count int, prefix string) [][]byte {
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%08d", prefix, i+1) // e.g., data-00000001
		keys[i] = []byte(key)
	}
	return keys
}

// Helper function to generate keys that are similar variations of a base string
func generateSimilarKeys(baseKey string, count int, swapPositions [][2]int) [][]byte {
	keysMap := make(map[string]bool) // Use map to ensure uniqueness
	baseBytes := []byte(baseKey)
	keysMap[baseKey] = true

	rng := rand.New(rand.NewSource(time.Now().UnixNano())) // For random swaps if needed

	for len(keysMap) < count {
		// Create variation
		newKeyBytes := make([]byte, len(baseBytes))
		copy(newKeyBytes, baseBytes)

		// Method 1: Predefined swaps (limited variations)
		if len(swapPositions) > 0 {
			idx := rng.Intn(len(swapPositions))
			p1, p2 := swapPositions[idx][0], swapPositions[idx][1]
			if p1 < len(newKeyBytes) && p2 < len(newKeyBytes) {
				newKeyBytes[p1], newKeyBytes[p2] = newKeyBytes[p2], newKeyBytes[p1]
			}
		} else {
			// Method 2: Random swaps (more variations, less predictable)
			if len(newKeyBytes) >= 2 {
				p1 := rng.Intn(len(newKeyBytes))
				p2 := rng.Intn(len(newKeyBytes))
				if p1 != p2 {
					newKeyBytes[p1], newKeyBytes[p2] = newKeyBytes[p2], newKeyBytes[p1]
				}
			}
		}
		keysMap[string(newKeyBytes)] = true
	}

	keys := make([][]byte, 0, len(keysMap))
	for k := range keysMap {
		keys = append(keys, []byte(k))
	}
	return keys
}

// Helper function to generate keys in parallel that are similar variations of a base string
func generateSimilarKeysParallel(baseKey string, count int, swapPositions [][2]int, numWorkers int) [][]byte {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() // Default to number of CPUs
	}

	// Create a thread-safe map to ensure uniqueness
	var keysMu sync.Mutex
	keysMap := make(map[string]bool, count)
	keysMap[baseKey] = true // Add base key

	// To keep track of when workers are done
	var wg sync.WaitGroup

	// Calculate keys per worker
	keysPerWorker := (count - 1) / numWorkers // -1 because we already have baseKey
	if keysPerWorker < 1 {
		keysPerWorker = 1
	}

	// Channel to collect keys from workers (with some buffer)
	resultChan := make(chan []byte, numWorkers*10)

	// Launch workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own RNG
			seed := time.Now().UnixNano() + int64(workerID)
			rng := rand.New(rand.NewSource(seed))

			// How many keys this worker should try to generate
			targetKeys := keysPerWorker
			if workerID == numWorkers-1 {
				// Last worker picks up any remainder
				targetKeys = count - 1 - (keysPerWorker * (numWorkers - 1))
			}

			// Generate keys until we have enough unique ones
			baseBytes := []byte(baseKey)
			keysGenerated := 0
			attempts := 0
			maxAttempts := targetKeys * 20 // Avoid infinite loops

			for keysGenerated < targetKeys && attempts < maxAttempts {
				attempts++

				// Create variation
				newKeyBytes := make([]byte, len(baseBytes))
				copy(newKeyBytes, baseBytes)

				// Method 1: Predefined swaps (limited variations)
				if len(swapPositions) > 0 {
					idx := rng.Intn(len(swapPositions))
					p1, p2 := swapPositions[idx][0], swapPositions[idx][1]
					if p1 < len(newKeyBytes) && p2 < len(newKeyBytes) {
						newKeyBytes[p1], newKeyBytes[p2] = newKeyBytes[p2], newKeyBytes[p1]
					}
				} else {
					// Method 2: Random swaps (more variations, less predictable)
					if len(newKeyBytes) >= 2 {
						p1 := rng.Intn(len(newKeyBytes))
						p2 := rng.Intn(len(newKeyBytes))
						if p1 != p2 {
							newKeyBytes[p1], newKeyBytes[p2] = newKeyBytes[p2], newKeyBytes[p1]
						}
					}
				}

				// Check if this key is unique (not in map yet)
				newKey := string(newKeyBytes)
				keysMu.Lock()
				if !keysMap[newKey] {
					keysMap[newKey] = true
					keysMu.Unlock()
					keysGenerated++
					resultChan <- newKeyBytes
				} else {
					keysMu.Unlock()
				}
			}
		}(i)
	}

	// Close the result channel when all workers complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect all generated keys
	keys := make([][]byte, 0, count)
	keys = append(keys, []byte(baseKey)) // Add the base key first

	for key := range resultChan {
		keys = append(keys, key)
		if len(keys) >= count {
			break
		}
	}

	return keys
}

// Helper function to generate values in parallel for the keys
func generateValuesParallel(count int, numWorkers int) [][]byte {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() // Default to number of CPUs
	}

	vals := make([][]byte, count)
	var wg sync.WaitGroup

	keysPerWorker := count / numWorkers
	if keysPerWorker < 1 {
		keysPerWorker = 1
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := workerID * keysPerWorker
			end := start + keysPerWorker

			// Last worker takes any remainder
			if workerID == numWorkers-1 {
				end = count
			}

			for j := start; j < end && j < count; j++ {
				vals[j] = []byte(fmt.Sprintf("v%d", j))
			}
		}(i)
	}

	wg.Wait()
	return vals
}

// Helper function to generate keys with low entropy (limited character set)
func generateLowEntropyKeys(count int, length int, alphabet string) [][]byte {
	keysMap := make(map[string]bool)
	alphabetBytes := []byte(alphabet)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for len(keysMap) < count {
		keyBytes := make([]byte, length)
		for i := 0; i < length; i++ {
			keyBytes[i] = alphabetBytes[rng.Intn(len(alphabetBytes))]
		}
		keysMap[string(keyBytes)] = true
	}

	keys := make([][]byte, 0, len(keysMap))
	for k := range keysMap {
		keys = append(keys, []byte(k))
	}
	return keys
}

// --- Parallel Build Tests ---

// TestBuildParallelBasicSuccess verifies that if at least one attempt succeeds,
// the build returns a valid CHD.
func TestBuildParallelBasicSuccess(t *testing.T) {
	b := Builder().Seed(1) // Use fixed seed for predictability if needed
	_, err := b.ParallelAttempts(3)
	require.NoError(t, err)
	// Default settings should allow success for sampleData

	// For this test we'll use a small sample dataset defined in main test file
	keys := make([][]byte, 0)
	vals := make([][]byte, 0)

	sampleData := map[string]string{
		"one":   "1",
		"two":   "2",
		"three": "3",
		"four":  "4",
		"five":  "5",
		"six":   "6",
		"seven": "7",
	}

	for k, v := range sampleData {
		keys = append(keys, []byte(k))
		vals = append(vals, []byte(v))
	}

	c, err := buildCHDFromSlices(t, keys, vals, b)
	require.NoError(t, err, "Build with parallel attempts failed unexpectedly")
	require.NotNil(t, c, "Returned CHD should not be nil on success")
	assert.Equal(t, len(keys), c.Len())

	// Verify content as a sanity check
	for i := range keys {
		val := c.Get(keys[i])
		assert.Equal(t, vals[i], val, "Mismatch for key %s", string(keys[i]))
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
	readTimeout := time.After(5 * time.Second)        // Safety timeout
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
					if !ok {
						break drainLoop
					} // Channel closed
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
	firstSuccessfulAttemptID := -1              // Reset before analysis
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

// TestBuildParallelReturnsOnFirstSuccess verifies that if one attempt succeeds,
// even if others fail, the build returns the success.
func TestBuildParallelReturnsOnFirstSuccess(t *testing.T) {
	keys := words[:100] // Larger dataset
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = []byte(fmt.Sprintf("v%d", i))
	}

	// Don't set a specific seed, let the logic generate them. We *expect* one
	// attempt to likely succeed before the other finishes all retries or buckets.

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

	// --- Collect messages concurrently with the build ---
	receivedProgress := make(map[int][]BuildProgress) // Map by AttemptID
	var progressMu sync.Mutex

	// Create a goroutine to collect progress messages
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for p := range progressChan {
			progressMu.Lock()
			receivedProgress[p.AttemptID] = append(receivedProgress[p.AttemptID], p)
			progressMu.Unlock()
		}
	}()

	// Now run the build
	c, buildErr := buildCHDFromSlices(t, keys, vals, builder)

	// We expect one attempt to succeed
	require.NoError(t, buildErr, "Build failed unexpectedly, cannot test cancellation effect")
	require.NotNil(t, c)

	// Now that the build is complete, close the channel and wait for collection to finish
	close(progressChan)
	<-progressDone

	// Analyze the collected messages
	firstSuccessfulAttemptID := -1
	for id, msgs := range receivedProgress {
		for _, p := range msgs {
			if p.Stage == "Complete" && firstSuccessfulAttemptID == -1 {
				firstSuccessfulAttemptID = id
				break
			}
		}
	}

	require.NotEqual(t, -1, firstSuccessfulAttemptID, "Expected one attempt to complete successfully")
}

// TestBuildParallelContextCancellation verifies that when one attempt succeeds,
// other attempts are cancelled and don't complete.
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
	for i := range keys {
		vals[i] = []byte(fmt.Sprintf("v%d", i))
	}

	// Don't set a specific seed, let the logic generate them. We *expect* one
	// attempt to likely succeed before the other finishes all retries or buckets.

	// --- Collect messages concurrently with the build ---
	receivedProgress := make(map[int][]BuildProgress) // Map by AttemptID
	var progressMu sync.Mutex

	// Create a goroutine to collect progress messages
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for p := range progressChan {
			progressMu.Lock()
			receivedProgress[p.AttemptID] = append(receivedProgress[p.AttemptID], p)
			progressMu.Unlock()
		}
	}()

	// Now run the build
	c, buildErr := buildCHDFromSlices(t, keys, vals, builder)

	// We expect one attempt to succeed
	require.NoError(t, buildErr, "Build failed unexpectedly, cannot test cancellation effect")
	require.NotNil(t, c)

	// Now that the build is complete, close the channel and wait for collection to finish
	close(progressChan)
	<-progressDone

	// Analyze the collected messages
	firstSuccessfulAttemptID := -1
	for id, msgs := range receivedProgress {
		for _, p := range msgs {
			if p.Stage == "Complete" && firstSuccessfulAttemptID == -1 {
				firstSuccessfulAttemptID = id
				break
			}
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
	t.Log("Parallel build with cancellation completed successfully")
}

// TestBuildParallelExtensive forces multiple parallel attempts with a constrained
// retry limit, verifying that parallelism allows the build to succeed and
// that cancellation stops non-winning attempts.
func TestBuildParallelExtensive(t *testing.T) {
	// Use a larger dataset
	numKeys := 50000
	base := "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ0123456789"
	t.Logf("Generating %d potentially tricky keys...", numKeys)
	
	// Use parallel generation with number of workers based on CPU count
	numWorkers := runtime.NumCPU()
	t.Logf("Using %d workers for parallel key generation", numWorkers)
	
	startTime := time.Now()
	keys := generateSimilarKeysParallel(base[:40], numKeys, nil, numWorkers) // Random swaps on 40 char base
	keyGenDuration := time.Since(startTime)
	
	// Generate values in parallel too
	startTime = time.Now()
	vals := generateValuesParallel(len(keys), numWorkers)
	valGenDuration := time.Since(startTime)
	
	t.Logf("Generated %d keys in %v (keys) and %v (values)", len(keys), keyGenDuration, valGenDuration)

	// Configure for extensive parallelism
	numAttempts := runtime.NumCPU() // Use all available CPU cores
	if numAttempts < 2 {
		numAttempts = 2 // Ensure at least 2 attempts for testing parallelism
	}
	// Lower retry limit significantly - adjust this based on observations if needed
	// Needs to be low enough to make single attempts potentially fail/slow,
	// but high enough that *some* seed is likely to succeed.
	retryLimit := 15000 // << TUNABLE PARAMETER
	
	// Estimate buffer size: numAttempts * buckets * avg_retries_per_bucket (roughly)
	// This is hard to predict, so oversize it.
	progressBufferSize := numAttempts * (len(keys)/2) / 5 // Heuristic, adjust if needed
	if progressBufferSize < 1000 { progressBufferSize = 1000 }
	
	progressChan := make(chan BuildProgress, progressBufferSize)

	builder := Builder().ProgressChan(progressChan)
	_, err := builder.ParallelAttempts(numAttempts)
	require.NoError(t, err)
	_, err = builder.RetryLimit(retryLimit)
	require.NoError(t, err)
	// No specific seed set - let the first attempt use time, others random.

	t.Logf("Starting build with %d parallel attempts, retry limit %d", numAttempts, retryLimit)

	// --- Run Build and Collect Progress Concurrently ---
	readDone := make(chan struct{})
	receivedProgress := make(map[int][]BuildProgress) // Map by AttemptID
	var progressMutex sync.Mutex // Protect map access from concurrent reads

	go func() {
		defer close(readDone)
		for p := range progressChan {
			progressMutex.Lock()
			fmt.Printf("DEBUG: Received progress message: AttemptID=%d, Stage=%s\n", p.AttemptID, p.Stage)
			receivedProgress[p.AttemptID] = append(receivedProgress[p.AttemptID], p)
			progressMutex.Unlock()
		}
		fmt.Println("DEBUG: Progress reader goroutine finished")
	}()

	buildDone := make(chan error)
	var builtCHD *CHD
	startTime = time.Now()
	go func() {
		builtCHD, err = buildCHDFromSlices(t, keys, vals, builder)
		// Signal build completion first
		buildDone <- err
		// THEN close progressChan to signal the reader there are no more messages
		close(progressChan)
	}()

	// Wait for build to finish
	buildErr := <-buildDone
	// Wait for reading goroutine to finish processing all messages
	<-readDone
	duration := time.Since(startTime)
	// --- End Build and Collect Progress ---

	t.Logf("Build finished in %v. Error: %v", duration, buildErr)

	// --- Assertions ---
	// Primary assertion: Build should succeed overall.
	require.NoError(t, buildErr, "Build expected to succeed due to parallel attempts finding a working seed within the retry limit")
	require.NotNil(t, builtCHD)
	assert.Equal(t, len(keys), builtCHD.Len()) // Verify size

	// Analyze collected progress
	progressMutex.Lock() // Lock map for final analysis
	defer progressMutex.Unlock()
	
	// Diagnostic logging to see the final state
	fmt.Println("DEBUG: Final state of receivedProgress map:")
	for id, msgs := range receivedProgress {
		if len(msgs) > 0 {
			lastMsg := msgs[len(msgs)-1]
			fmt.Printf("DEBUG:   AttemptID=%d, messages=%d, last stage=%s\n", 
				id, len(msgs), lastMsg.Stage)
		} else {
			fmt.Printf("DEBUG:   AttemptID=%d, no messages\n", id)
		}
	}

	require.GreaterOrEqualf(t, len(receivedProgress), 1, "Should have received progress from at least one attempt (got %d)", len(receivedProgress))
	// Ideally, we see progress from multiple attempts if the first didn't succeed instantly
	t.Logf("Received progress reports from %d distinct attempts.", len(receivedProgress))

	firstSuccessfulAttemptID := -1
	completedAttempts := 0
	cancelledAttempts := 0
	maxBucketProcessed := 0 // Track overall progress

	for id, msgs := range receivedProgress {
		if len(msgs) == 0 {
			t.Logf("Warning: Attempt %d sent no progress messages.", id)
			continue
		}
		lastMsg := msgs[len(msgs)-1]

		// Track max progress
		if lastMsg.BucketsProcessed > maxBucketProcessed {
			maxBucketProcessed = lastMsg.BucketsProcessed
		}

		// Identify completed or near-completed attempts
		if lastMsg.Stage == "Complete" {
			// This should be the winner
			if firstSuccessfulAttemptID == -1 { // Record the first one found
				firstSuccessfulAttemptID = id
			}
			assert.Equal(t, lastMsg.TotalBuckets, lastMsg.BucketsProcessed, "Completed attempt %d should have processed all buckets", id)
			completedAttempts++ // Count how many *claimed* to complete
		} else {
			// Didn't complete
			cancelledAttempts++
			t.Logf("Attempt %d likely cancelled (last stage: %s, buckets: %d/%d)", id, lastMsg.Stage, lastMsg.BucketsProcessed, lastMsg.TotalBuckets)
		}
	}
	
	assert.NotEqualf(t, -1, firstSuccessfulAttemptID, "Expected one attempt (%d total attempts ran) to complete successfully by reaching 'Complete' stage", len(receivedProgress))
	assert.Equal(t, 1, completedAttempts, "Expected exactly one attempt to report 'Complete' stage (found %d)", completedAttempts)
	// Check that if multiple attempts sent progress, some were cancelled
	if len(receivedProgress) > 1 {
		// The number of attempts that didn't complete should match cancelledAttempts
		expectedCancelled := len(receivedProgress) - completedAttempts
		assert.GreaterOrEqual(t, cancelledAttempts, expectedCancelled, "Expected non-winning attempts to be cancelled")
	}

	t.Logf("Analysis complete. Winning attempt: %d. Completed: %d. Cancelled: %d. Max buckets reached by any process: %d",
		firstSuccessfulAttemptID, completedAttempts, cancelledAttempts, maxBucketProcessed)
}

// TestBuildWithDifficultDataset attempts to build using a dataset designed
// to potentially cause more hash collisions, testing the retry limit and parallel attempts.
func TestBuildWithDifficultDataset(t *testing.T) {
	// --- Generate Difficult Keys ---
	// Choose one or combine strategies. Let's use similar keys for this test
	numKeys := 10000 // Start with a moderate number for faster test runs
	t.Logf("Generating %d difficult keys...", numKeys)

	base := "abcdefghijklmnopqrstuvwxyz0123456789"
	
	// Use parallel generation with number of workers based on CPU count
	numWorkers := runtime.NumCPU()
	t.Logf("Using %d workers for parallel key generation", numWorkers)
	
	startTime := time.Now()
	difficultKeys := generateSimilarKeysParallel(base[:16], numKeys, nil, numWorkers) // Random swaps on 16 char base
	keyGenDuration := time.Since(startTime)

	// Generate values in parallel too
	startTime = time.Now()
	difficultVals := generateValuesParallel(len(difficultKeys), numWorkers)
	valGenDuration := time.Since(startTime)
	
	t.Logf("Generated %d keys in %v (keys) and %v (values)", len(difficultKeys), keyGenDuration, valGenDuration)

	// --- Test Scenario 1: Low Retry Limit (Expect Failure) ---
	t.Run("LowRetryLimit", func(t *testing.T) {
		lowLimit := 100                // Intentionally very low limit
		builder := Builder().Seed(123) // Fixed seed
		_, err := builder.RetryLimit(lowLimit)
		require.NoError(t, err)
		_, err = builder.ParallelAttempts(1) // Single attempt
		require.NoError(t, err)

		startTime := time.Now()
		_, buildErr := buildCHDFromSlices(t, difficultKeys, difficultVals, builder)
		duration := time.Since(startTime)
		t.Logf("LowRetryLimit build duration: %v", duration)

		require.Error(t, buildErr, "Build should fail with a low retry limit on this dataset")
		assert.Contains(t, buildErr.Error(), fmt.Sprintf("failed to find a collision-free hash function after ~%d attempts", lowLimit))
	})

	// --- Test Scenario 2: Increased Retry Limit (Expect Success) ---
	t.Run("IncreasedRetryLimit", func(t *testing.T) {
		// Use a significantly higher limit, but less than default to keep test duration reasonable
		increasedLimit := 500_000
		builder := Builder().Seed(123) // Same fixed seed
		_, err := builder.RetryLimit(increasedLimit)
		require.NoError(t, err)
		_, err = builder.ParallelAttempts(1) // Single attempt
		require.NoError(t, err)

		startTime := time.Now()
		c, buildErr := buildCHDFromSlices(t, difficultKeys, difficultVals, builder)
		duration := time.Since(startTime)
		t.Logf("IncreasedRetryLimit build duration: %v", duration)

		require.NoError(t, buildErr, "Build should succeed with an increased retry limit")
		require.NotNil(t, c)
		assert.Equal(t, len(difficultKeys), c.Len())
	})

	// --- Test Scenario 3: Default Limit + Parallel Attempts (Expect Success) ---
	t.Run("ParallelAttempts", func(t *testing.T) {
		numAttempts := 4               // Use multiple attempts
		builder := Builder().Seed(123) // Use same initial seed (others will be random)
		// Use default retry limit (10M) - one attempt *might* fail but others should succeed
		_, err := builder.ParallelAttempts(numAttempts)
		require.NoError(t, err)

		startTime := time.Now()
		c, buildErr := buildCHDFromSlices(t, difficultKeys, difficultVals, builder)
		duration := time.Since(startTime)
		t.Logf("ParallelAttempts build duration: %v", duration)

		require.NoError(t, buildErr, "Build should succeed using parallel attempts, even if some seeds are difficult")
		require.NotNil(t, c)
		assert.Equal(t, len(difficultKeys), c.Len())
	})
}
