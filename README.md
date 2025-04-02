# Minimal Perfect Hashing for Go (aelaguiz Fork)

[![Go Report Card](https://goreportcard.com/badge/github.com/aelaguiz/mph)](https://goreportcard.com/report/github.com/aelaguiz/mph)
[![GoDoc](https://godoc.org/github.com/aelaguiz/mph?status.svg)](https://godoc.org/github.com/aelaguiz/mph)
<!-- Add License badge if you have a LICENSE file -->
<!-- [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) -->

This library provides [Minimal Perfect Hashing](http://en.wikipedia.org/wiki/Perfect_hash_function) (MPH) for Go using the [Compress, Hash and Displace](http://cmph.sourceforge.net/papers/esa09.pdf) (CHD) algorithm.

This is a fork of the original `github.com/alecthomas/mph` library, enhanced with tunable build parameters, progress reporting, and robust parallel build capabilities.

## Fork Information

This repository is a fork of the excellent [`github.com/alecthomas/mph`](https://github.com/alecthomas/mph) library.

**Original author:** Alec Thomas ([`github.com/alecthomas`](https://github.com/alecthomas))

This fork retains the core CHD algorithm and API structure but adds the following major enhancements aimed at improving build robustness, performance, and observability, especially for large datasets:

1.  **Tunable Build Parameters:** Control over `BucketRatio` and `RetryLimit` during MPH construction.
2.  **Progress Reporting:** An optional channel mechanism (`ProgressChan`) to receive detailed updates during the build process.
3.  **Robust Parallel Builds:** Ability to run multiple build attempts concurrently (`ParallelAttempts`) using different random seeds. The first successful attempt returns immediately, and other attempts are cancelled, significantly increasing the chance of success for difficult datasets without manual intervention.
4.  **Reproducible Builds:** Consistent `Seed()` method for deterministic output.

## What is this useful for?

(Content from original README)

Primarily, extremely efficient access to potentially very large static datasets, such as geographical data, NLP data sets, etc.

On my 2012 vintage MacBook Air, a benchmark against a wikipedia index with 300K keys against a 2GB TSV dump takes about ~200ns per lookup. (Note: Performance may vary on different hardware and with different datasets).

## How would it be used?

(Content from original README)

Typically, the table would be used as a fast index into a (much) larger data set, with values in the table being file offsets or similar.

The tables can be serialized. Numeric values are written in little endian form.

## Basic Example Code

(Adapted from original README)

Building and serializing an MPH hash table (error checking omitted for clarity):

```go
package main

import (
	"fmt"
	"os"

	"github.com/aelaguiz/mph" // Use the fork's import path
)

func main() {
	data := map[string]string{
		"one":   "1",
		"two":   "2",
		"three": "3",
	}

	builder := mph.Builder() // Start with the builder
	for k, v := range data {
		// Keys and values must be []byte
		builder.Add([]byte(k), []byte(v))
	}

	// Build the table (can add options before this)
	h, err := builder.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to build MPH: %v", err))
	}

	// Serialize to a file
	w, err := os.Create("data.idx")
	if err != nil {
		panic(err)
	}
	defer w.Close()

	err = h.Write(w)
	if err != nil {
		panic(fmt.Sprintf("Failed to write MPH: %v", err))
	}

	fmt.Println("MPH table built and saved successfully.")
}
```

Deserializing the hash table and performing lookups:

```go
package main

import (
	"fmt"
	"os"

	"github.com/aelaguiz/mph" // Use the fork's import path
)

func main() {
	r, err := os.Open("data.idx")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	// Read the entire table (for smaller tables)
	h, err := mph.Read(r)
	if err != nil {
		panic(fmt.Sprintf("Failed to read MPH: %v", err))
	}

	// Or Mmap for large tables (assuming 'b' is your memory-mapped byte slice)
	// h, err := mph.Mmap(b)

	keyToLookup := []byte("two")
	v := h.Get(keyToLookup)

	if v == nil {
		fmt.Printf("Key '%s' not found\n", string(keyToLookup))
	} else {
		fmt.Printf("Value for key '%s': %s\n", string(keyToLookup), string(v))
	}

    keyToLookup = []byte("four")
    v = h.Get(keyToLookup)
    if v == nil {
        fmt.Printf("Key '%s' not found\n", string(keyToLookup))
    }
}

```

MMAP is also indirectly supported, by deserializing from a byte slice (`mph.Mmap(byteSlice)`) which allows the keys and values to be sliced directly from the underlying mapped memory without copying.

## Enhancements in Detail (Fork Features)

This fork provides additional methods on the `mph.CHDBuilder` to control and observe the build process:

### Tunable Parameters

Control the internal build heuristics:

*   `Seed(seed int64)`: Sets the initial random seed for reproducible builds. If not called, a time-based seed is used for the first attempt.
*   `BucketRatio(ratio float64)`: Sets the ratio of initial hash buckets to the number of keys (m/n). Default is `0.5`. Values like `1.0` might sometimes help with difficult datasets. Must be > 0.0. Returns an error if invalid.
*   `RetryLimit(limit int)`: Sets the maximum attempts to find a collision-free hash function for a single bucket before failing. Default is `10,000,000`. Must be > 0. Returns an error if invalid.

**Example:**

```go
builder := mph.Builder().
    Seed(12345).              // Ensure reproducible build
    BucketRatio(1.0)          // Use n buckets instead of n/2
if err != nil { panic(err) }
builder, err = builder.RetryLimit(5000000)  // Lower retry limit
if err != nil { panic(err) }

h, err := builder.Build()
// ...
```

### Progress Reporting

For potentially long-running builds, especially with large datasets or when using parallel attempts, you can monitor the progress of the `Build()` operation.

To enable progress reporting:

1.  Create a Go channel of type `mph.BuildProgress`. **It is highly recommended to use a buffered channel** to prevent the build process from blocking if the channel isn't read immediately.
2.  Pass this channel to the builder using the `.ProgressChan()` method.
3.  Launch a separate goroutine to read progress updates from the channel.
4.  Call `Build()`. The `mph` library will send `BuildProgress` structs to the channel at various stages.
5.  The library will **automatically close** the progress channel when the `Build()` operation (including all parallel attempts, if applicable) is fully complete. Your reading goroutine can use a `for range` loop, which will terminate gracefully when the channel is closed.

The `BuildProgress` struct contains the following fields:

*   `AttemptID`: An integer identifying which parallel build attempt this update belongs to (starts at 1). If `ParallelAttempts` is 1, this will always be 1.
*   `Stage`: A string describing the current phase (e.g., "Hashing Keys", "Sorting Buckets", "Assigning Hashes", "Packing Data", "Complete").
*   `TotalBuckets`: The total number of initial buckets determined by the `BucketRatio`.
*   `BucketsProcessed`: The number of buckets for which hash functions have been successfully assigned so far (relevant during the "Assigning Hashes" stage).
*   `CurrentBucketSize`: The number of keys in the specific bucket currently being processed (relevant during "Assigning Hashes").
*   `CurrentBucketCollisions`: The number of hash functions tried for the *current* bucket before finding a collision-free one (relevant during "Assigning Hashes").

**Example:**

```go
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/aelaguiz/mph" // Import path for this fork
)

func main() {
	keys := [][]byte{
		[]byte("apple"), []byte("banana"), []byte("cherry"), []byte("date"),
		[]byte("elderberry"), []byte("fig"), []byte("grape"), []byte("honeydew"),
		// Add more keys for a longer build to see more progress
	}
	values := make([][]byte, len(keys))
	for i := range keys {
		values[i] = []byte(fmt.Sprintf("value_for_%s", keys[i]))
	}

	// 1. Create a buffered channel
	//    Size buffer appropriately for expected update frequency vs consumption speed.
	progressChan := make(chan mph.BuildProgress, 100)

	// 2. Configure the builder and set the progress channel
	builder := mph.Builder().
		ProgressChan(progressChan). // <-- Set the channel
		Seed(12345)                 // Use a fixed seed for predictable output (optional)
		// Optionally configure ParallelAttempts(n) here too

	// Add data
	for i := range keys {
		builder.Add(keys[i], values[i])
	}

	// 3. Launch a goroutine to read progress updates
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Progress Monitor: Started")
		startTime := time.Now()
		for p := range progressChan { // Loop terminates when channel is closed by Build()
			fmt.Printf("Progress [Attempt %d] (%s): Stage='%s', Buckets=%d/%d, Collisions=%d\n",
				p.AttemptID,
				time.Since(startTime).Round(time.Millisecond),
				p.Stage,
				p.BucketsProcessed,
				p.TotalBuckets,
				p.CurrentBucketCollisions,
			)
		}
		fmt.Println("Progress Monitor: Finished (Channel Closed)")
	}()

	// 4. Start the build
	fmt.Println("Main: Starting Build...")
	startTime := time.Now()
	c, err := builder.Build()
	buildDuration := time.Since(startTime)
	fmt.Printf("Main: Build Finished in %s\n", buildDuration)

	// 5. Handle build result
	if err != nil {
		fmt.Printf("Main: Build failed: %v\n", err)
	} else {
		fmt.Printf("Main: Build successful! Table size: %d\n", c.Len())
		// ... use the table c.Get(...) ...
		val := c.Get([]byte("apple"))
		fmt.Printf("Main: Get('apple'): %s\n", string(val))
	}

	// 6. Wait for the progress reader goroutine to finish processing all messages
	fmt.Println("Main: Waiting for progress monitor to exit...")
	wg.Wait()
	fmt.Println("Main: Exiting.")
}
```

**Notes:**

*   Consuming progress updates in a separate goroutine is essential to avoid blocking the build process, especially if using an unbuffered or small-buffered channel.
*   If using `ParallelAttempts(n)` with `n > 1`, you will see updates with different `AttemptID` values potentially interleaved in the channel. The `Build()` function itself will only return the result from the *first* successful attempt and signal the others to cancel.

### Parallel Builds

Automatically try multiple random seeds in parallel to increase the chance of success for large or difficult datasets:

*   `ParallelAttempts(n int)`: Set the number of build attempts to run concurrently. Default is `1`. Must be >= 1. Returns an error if invalid.

When `n > 1`, `Build()` launches `n` goroutines. Each uses a different seed (the first uses the seed provided by `Seed()` or time, others use generated random seeds). The first goroutine to successfully build the table returns its result. The `Build()` function then signals the other attempts to cancel via context. `buildInternal` checks this context periodically and attempts to exit early.

**Example:**

```go
import "runtime"

// Use available CPU cores for attempts, useful for large builds
numAttempts := runtime.NumCPU()
if numAttempts < 1 { numAttempts = 1 }

// Lower retry limit, relying on parallelism to find a working seed
builder := mph.Builder().
    ParallelAttempts(numAttempts).
    RetryLimit(1_000_000) // Lower limit, maybe compensated by parallelism
if err != nil { panic(err) }

h, err := builder.Build() // Will run up to numAttempts builds concurrently
// ... handle result ...
```

## API Documentation

The full [API documentation](https://godoc.org/github.com/aelaguiz/mph) has more details.

## License

This project is licensed under the terms of the original `github.com/alecthomas/mph` library's license. Please include the original LICENSE file in this fork. (Likely MIT or similar).