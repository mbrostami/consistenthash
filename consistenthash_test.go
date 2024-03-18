package consistenthash

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(WithDefaultReplicas(3), WithHashFunc(crc32.ChecksumIEEE))

	// Given the above hash function, this will give replicas with "hashes":
	hash.Add([]byte("6"), []byte("4"), []byte("2"))
	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"47": "6",
		"63": "2",
	}

	for k, v := range testCases {
		n := hash.GetString(k)
		if n != v {
			t.Errorf("Asking for %s, should have yielded %s but got %s", k, v, n)
		}
	}

	// 6,61,62,4,41,42,2,21,22,8,81,82
	hash.Add([]byte("8"))

	// 27 should now map to 8.
	testCases["63"] = "8"

	for k, v := range testCases {
		n := hash.GetString(k)
		if n != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// 6,61,62,4,41,42,2,21,22
	hash.Remove([]byte("8"))

	// 27 should now map to 2 because 8 is the last node which is removed.
	testCases["63"] = "2"

	for k, v := range testCases {
		if hash.GetString(k) != v {
			t.Errorf("Asking for %s, should have yielded %s, got %s", k, v, hash.GetString(k))
		}
	}
}

func TestReplication(t *testing.T) {

	// Default replica is 1
	hash := New(WithDefaultReplicas(1), WithHashFunc(func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	}))

	// 6,4,2
	hash.Add([]byte("6"), []byte("4"), []byte("2"))

	testCases := map[string]string{
		"1": "2",
		"3": "4",
		"5": "6",
		"6": "6",
		"7": "2", // 7 goes back to the first hash in circle
	}

	for k, v := range testCases {
		n := hash.GetString(k)
		if n != v {
			t.Errorf("Asking for %s, should have yielded %s got %s", k, v, hash.GetString(k))
		}
	}

	// This will generates 3 hashes 6,61,62,4,2
	hash.AddReplicas(3, []byte("6"))

	// 11,23 should now map to 6 because 26 is the last node which is kind of hash of (6 + 3)
	testCases["7"] = "6" // 7 now goes to the next available hash in circle which is 61,

	for k, v := range testCases {
		if hash.GetString(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

func TestConcurrency(t *testing.T) {
	hash := New()
	items := []string{"Bill", "Bob", "Bonny", "Bob", "Bill", "Bony", "Bob"}
	for _, item := range items {
		go func(it []byte) {
			hash.Add(it)
		}([]byte(item))
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New()
	hash2 := New()

	hash1.Add([]byte("Bill"))
	hash1.Add([]byte("Bob"))
	hash1.Add([]byte("Bonny"))
	hash2.Add([]byte("Bob"))
	hash2.Add([]byte("Bonny"))
	hash2.Add([]byte("Bill"))

	if hash1.GetString("Ben") != hash2.GetString("Ben") {
		t.Errorf("fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add([]byte("Becky"))
	hash2.Add([]byte("Ben"))
	hash2.Add([]byte("Bobby"))

	if hash1.GetString("Bill") != hash2.GetString("Bill") ||
		hash1.GetString("Bob") != hash2.GetString("Bob") ||
		hash1.GetString("Bonny") != hash2.GetString("Bonny") {
		t.Errorf("direct matches should always return the same entry")
	}

}

func BenchmarkGetBytes8(b *testing.B)       { benchmarkGetBytes(b, 8, false, 0) }
func BenchmarkGetBytes512(b *testing.B)     { benchmarkGetBytes(b, 512, false, 0) }
func BenchmarkGetBytes1024(b *testing.B)    { benchmarkGetBytes(b, 1024, false, 0) }
func BenchmarkGetBytes4096nbp(b *testing.B) { benchmarkGetBytes(b, 4096, false, 0) }
func BenchmarkGetBytes4096bp(b *testing.B)  { benchmarkGetBytes(b, 4096, false, 1) }
func BenchmarkGetBytes4096bp5(b *testing.B) { benchmarkGetBytes(b, 4096, false, 5) }

func BenchmarkAdd128nbp(b *testing.B)  { benchmarkAdd(b, 128, false, 0) }
func BenchmarkAdd128bp(b *testing.B)   { benchmarkAdd(b, 128, false, 5) }
func BenchmarkAdd128bp10(b *testing.B) { benchmarkAdd(b, 128, false, 20) }

func BenchmarkGetBytes200000NoPartitioning(b *testing.B)   { benchmarkGetBytes(b, 200000, false, 0) }
func BenchmarkGetBytes200000WithPartitioning(b *testing.B) { benchmarkGetBytes(b, 200000, false, 5) }

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8, false) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512, false) }

func benchmarkGet(b *testing.B, shards int, readLockFree bool) {

	hash := New(WithDefaultReplicas(50), WithReadLockFree(readLockFree))

	var buckets []string
	var lookups []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
		hash.Add([]byte(fmt.Sprintf("shard-%d", i)))
		lookups = append(lookups, fmt.Sprintf("shard-x-%d", i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.GetString(lookups[i&(shards-1)])
	}
}

func benchmarkAdd(b *testing.B, shards int, readLockFree bool, blockPartitionDivision int) {

	hash := New(
		WithDefaultReplicas(50*uint(shards)),
		WithReadLockFree(readLockFree),
		WithBlockPartitioning(blockPartitionDivision),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var str bytes.Buffer
		str.WriteString(fmt.Sprintf("%d", i))
		hash.Add(str.Bytes())
	}
}

func benchmarkGetBytes(b *testing.B, shards int, readLockFree bool, blockPartitionDivision int) {
	hash := New(
		WithDefaultReplicas(50),
		WithReadLockFree(readLockFree),
		WithBlockPartitioning(blockPartitionDivision),
	)
	var lookups [][]byte
	var buckets [][]byte
	for i := 0; i < shards; i++ {
		var str bytes.Buffer
		buckets = append(buckets, []byte(fmt.Sprintf("%d", i)))
		str.WriteString(fmt.Sprintf("shard-x-%d", i))
		lookups = append(lookups, str.Bytes())
	}
	hash.Add(buckets...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.Get(lookups[i&(shards-1)])
	}
}
