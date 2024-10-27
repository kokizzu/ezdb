package ezdb

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// TestEZDB_BasicOperations tests basic put and get operations.
func TestEZDB_BasicOperations(t *testing.T) {
	// Remove any existing test database.
	os.RemoveAll("testdb_basic")
	defer os.RemoveAll("testdb_basic")

	// Create a new database client.
	db, err := New("testdb_basic")
	if err != nil {
		t.Fatalf("Failed to create new db client: %v", err)
	}
	defer db.Close()

	// Create a new database reference.
	ref, err := NewRef[string, string]("test_ref", db)
	if err != nil {
		t.Fatalf("Failed to create new db ref: %v", err)
	}

	// Test data.
	key := "hello"
	value := "world"

	// Put the key-value pair.
	err = ref.Put(&key, &value)
	if err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	// Get the value.
	valueOut, err := ref.Get(&key)
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}

	if *valueOut != value {
		t.Errorf("Expected value %s, got %s", value, *valueOut)
	}
}

// TestEZDB_ConcurrentAccess tests concurrent accesses to the database.
func TestEZDB_ConcurrentAccess(t *testing.T) {
	// Remove any existing test database.
	os.RemoveAll("testdb_concurrent")
	defer os.RemoveAll("testdb_concurrent")

	// Create a new database client with increased number of readers.
	db, err := New("testdb_concurrent", WithNumReaders(100))
	if err != nil {
		t.Fatalf("Failed to create new db client: %v", err)
	}
	defer db.Close()

	// Create a new database reference.
	ref, err := NewRef[int, string]("test_ref_concurrent", db)
	if err != nil {
		t.Fatalf("Failed to create new db ref: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 50
	numIterations := 100

	// Concurrent writes.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		i := i // Capture loop variable.
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				key := i*numIterations + j
				value := fmt.Sprintf("value-%d", key)
				err := ref.Put(&key, &value)
				if err != nil {
					t.Errorf("Failed to put key %d: %v", key, err)
				}
			}
		}()
	}

	wg.Wait()

	// Concurrent reads.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		i := i // Capture loop variable.
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				key := i*numIterations + j
				valueOut, err := ref.Get(&key)
				if err != nil {
					t.Errorf("Failed to get key %d: %v", key, err)
					continue
				}
				expectedValue := fmt.Sprintf("value-%d", key)
				if *valueOut != expectedValue {
					t.Errorf("For key %d, expected value %s, got %s", key, expectedValue, *valueOut)
				}
			}
		}()
	}

	wg.Wait()
}

// TestEZDB_StressTest performs a stress test with heavy concurrent operations.
func TestEZDB_StressTest(t *testing.T) {
	os.RemoveAll("testdb_stress")
	defer os.RemoveAll("testdb_stress")

	db, err := New("testdb_stress", WithNumReaders(100))
	if err != nil {
		t.Fatalf("Failed to create new db client: %v", err)
	}
	defer db.Close()

	ref, err := NewRef[int, []byte]("test_ref_stress", db)
	if err != nil {
		t.Fatalf("Failed to create new db ref: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 1000

	// Stress test with concurrent writes and reads.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		i := i // Capture loop variable.
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := i*numOperations + j
				value := bytes.Repeat([]byte{byte(key % 256)}, 1024) // 1KB value.
				err := ref.Put(&key, &value)
				if err != nil {
					t.Errorf("Failed to put key %d: %v", key, err)
				}

				// Optionally test reads.
				valueOut, err := ref.Get(&key)
				if err != nil {
					t.Errorf("Failed to get key %d: %v", key, err)
					continue
				}
				if !bytes.Equal(*valueOut, value) {
					t.Errorf("For key %d, values do not match", key)
				}
			}
		}()
	}

	wg.Wait()
}

// FuzzEZDB performs fuzz testing on the database operations.
func FuzzEZDB(f *testing.F) {
	// Remove any existing test database.
	os.RemoveAll("testdb_fuzz")
	defer os.RemoveAll("testdb_fuzz")

	db, err := New("testdb_fuzz", WithLogger(zerolog.Nop()))
	if err != nil {
		f.Fatalf("Failed to create new db client: %v", err)
	}
	defer db.Close()

	ref, err := NewRef[string, string]("test_ref_fuzz", db)
	if err != nil {
		f.Fatalf("Failed to create new db ref: %v", err)
	}

	// Seed corpus.
	f.Add("key", "value")

	f.Fuzz(func(t *testing.T, key string, value string) {
		if len(key) == 0 || len(value) == 0 {
			return
		}

		// Put the key-value pair.
		err := ref.Put(&key, &value)
		if err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}

		// Get the value.
		valueOut, err := ref.Get(&key)
		if err != nil {
			t.Fatalf("Failed to get data: %v", err)
		}

		if *valueOut != value {
			t.Errorf("Expected value %s, got %s", value, *valueOut)
		}
	})
}
