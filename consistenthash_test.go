package consistenthash

import (
	"fmt"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := NewConsistentHash(3, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6")
	hash.Add("4")
	hash.Add("2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	hash.Remove("8")

	// 27 should now map to 2 because 8 is the last node which is removed.
	testCases["27"] = "2"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s, got %s", k, v, hash.Get(k))
		}
	}
}

func TestReplication(t *testing.T) {

	// Default replica is 1
	hash := NewConsistentHash(1, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	hash.Add("6")
	hash.Add("4")
	hash.Add("2")

	testCases := map[string]string{
		"2":  "2",
		"3":  "4",
		"5":  "6",
		"6":  "6",
		"11": "2",
		"23": "2",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// This will generates 3 hashes 6,16,26
	hash.AddReplicas("6", 3)

	// 11,23 should now map to 6 because 26 is the last node which is kind of hash of (6 + 3)
	testCases["11"] = "6"
	testCases["23"] = "6"
	testCases["27"] = "2" // last hash is 26 so it should begin the ring from 2

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

func TestConsistency(t *testing.T) {
	hash1 := NewConsistentHash(1, nil)
	hash2 := NewConsistentHash(1, nil)

	hash1.Add("Bill")
	hash1.Add("Bob")
	hash1.Add("Bonny")
	hash2.Add("Bob")
	hash2.Add("Bonny")
	hash2.Add("Bill")

	if hash1.Get("Ben") != hash2.Get("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add("Becky")
	hash2.Add("Ben")
	hash2.Add("Bobby")

	if hash1.Get("Ben") != hash2.Get("Ben") ||
		hash1.Get("Bob") != hash2.Get("Bob") ||
		hash1.Get("Bonny") != hash2.Get("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}

}

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8) }
func BenchmarkGet32(b *testing.B)  { benchmarkGet(b, 32) }
func BenchmarkGet128(b *testing.B) { benchmarkGet(b, 128) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512) }

func benchmarkGet(b *testing.B, shards int) {

	hash := NewConsistentHash(50, nil)

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
		hash.Add(fmt.Sprintf("shard-%d", i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}
