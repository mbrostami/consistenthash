package consistenthash

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(WithDefaultReplicas(3), WithHashFunc(func(key []byte) uint32 {
		str := string(key[0])
		if len(key) > 4 {
			n := 0
			for i := 1; i < len(key); i++ {
				n += int(key[i])
			}
			if n != 0 {
				str += strconv.Itoa(n)
			}
		} else {
			for i := 1; i < len(key); i++ {
				str += string(key[i])
			}
		}
		i, _ := strconv.Atoi(str)
		return uint32(i)
	}))

	// Given the above hash function, this will give replicas with "hashes":
	// 2,4,6,21,22,41,42,61,62
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
		str := string(key[0])
		if len(key) > 4 {
			n := 0
			for i := 1; i < len(key); i++ {
				n += int(key[i])
			}
			if n != 0 {
				str += strconv.Itoa(n)
			}
		} else {
			for i := 1; i < len(key); i++ {
				str += string(key[i])
			}
		}
		i, _ := strconv.Atoi(str)
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

func BenchmarkGet8x50(b *testing.B)      { benchmarkGet(b, 8, false, 0) }
func BenchmarkGet512x50(b *testing.B)    { benchmarkGet(b, 512, false, 0) }
func BenchmarkGet1024x50(b *testing.B)   { benchmarkGet(b, 1024, false, 0) }
func BenchmarkGet4096x50(b *testing.B)   { benchmarkGet(b, 4096, false, 0) }
func BenchmarkGet200000x50(b *testing.B) { benchmarkGet(b, 200000, false, 0) }
func BenchmarkAdd8x50(b *testing.B)      { benchmarkAdd(b, 8, false, 0) }
func BenchmarkAddBulk8x50(b *testing.B)  { benchmarkBulkAdd(b, 8, false, 0) }
func BenchmarkRemove128x50(b *testing.B) { benchmarkRemove(b, 128, false, 0) }

func BenchmarkGet8BPx50(b *testing.B)      { benchmarkGet(b, 8, false, 5) }
func BenchmarkGet512BPx50(b *testing.B)    { benchmarkGet(b, 512, false, 5) }
func BenchmarkGet1024BPx50(b *testing.B)   { benchmarkGet(b, 1024, false, 5) }
func BenchmarkGet4096BPx50(b *testing.B)   { benchmarkGet(b, 4096, false, 5) }
func BenchmarkGet200000BPx50(b *testing.B) { benchmarkGet(b, 200000, false, 5) }
func BenchmarkAdd8BPx50(b *testing.B)      { benchmarkAdd(b, 8, false, 5) }
func BenchmarkAddBulk8BPx50(b *testing.B)  { benchmarkBulkAdd(b, 8, false, 5) }
func BenchmarkRemove128BPx50(b *testing.B) { benchmarkRemove(b, 128, false, 5) }

func BenchmarkGet8BPx50_100(b *testing.B)      { benchmarkGet(b, 8, false, 100) }
func BenchmarkGet512BPx50_100(b *testing.B)    { benchmarkGet(b, 512, false, 100) }
func BenchmarkGet1024BPx50_100(b *testing.B)   { benchmarkGet(b, 1024, false, 100) }
func BenchmarkGet4096BPx50_100(b *testing.B)   { benchmarkGet(b, 4096, false, 100) }
func BenchmarkGet200000BPx50_100(b *testing.B) { benchmarkGet(b, 200000, false, 100) }

func BenchmarkStringGet8x50(b *testing.B)   { benchmarkGetString(b, 8, false) }
func BenchmarkStringGet512x50(b *testing.B) { benchmarkGetString(b, 512, false) }

func benchmarkGetString(b *testing.B, shards int, readLockFree bool) {

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

func benchmarkBulkAdd(b *testing.B, shards int, readLockFree bool, blockPartitionDivision int) {

	hash := New(
		WithDefaultReplicas(50),
		WithReadLockFree(readLockFree),
		WithBlockPartitioning(blockPartitionDivision),
	)
	var buckets [][]byte
	for i := 0; i <= shards; i++ {
		buckets = append(buckets, []byte(fmt.Sprintf("%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.Add(buckets...)
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

func benchmarkRemove(b *testing.B, shards int, readLockFree bool, blockPartitionDivision int) {

	hash := New(
		WithDefaultReplicas(50),
		WithReadLockFree(readLockFree),
		WithBlockPartitioning(blockPartitionDivision),
	)
	var buckets [][]byte
	for i := 0; i <= shards; i++ {
		buckets = append(buckets, []byte(fmt.Sprintf("%d", i)))
	}
	hash.Add(buckets...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.Remove(buckets[i%(shards-1)])
	}
}

func benchmarkGet(b *testing.B, shards int, readLockFree bool, blockPartitionDivision int) {
	hash := New(
		WithDefaultReplicas(50),
		WithReadLockFree(readLockFree),
		WithBlockPartitioning(blockPartitionDivision),
		WithMetrics(),
	)
	var lookups [][]byte
	var buckets [][]byte
	for i := 0; i <= shards; i++ {
		var str bytes.Buffer
		buckets = append(buckets, []byte(fmt.Sprintf("%d", i)))
		str.WriteString(fmt.Sprintf("shard-x-%d", i))
		lookups = append(lookups, str.Bytes())
	}
	hash.Add(buckets...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.Get(lookups[i%(shards-1)])
	}
	b.Logf("missed: %+v", hash.Metrics())
}
