package consistenthash

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"testing"
)

// referencePoints recomputes the exact ring positions add() generates for a key
// under the default crc32 hash: originalHash, then hash(key || le32(i)).
func referencePoints(key []byte, replicas uint) []uint32 {
	pts := []uint32{crc32.ChecksumIEEE(key)}
	var h bytes.Buffer
	for i := uint32(1); i < uint32(replicas); i++ {
		h.Reset()
		h.Write(key)
		h.WriteByte(byte(i))
		h.WriteByte(byte(i >> 8))
		h.WriteByte(byte(i >> 16))
		h.WriteByte(byte(i >> 24))
		pts = append(pts, crc32.ChecksumIEEE(h.Bytes()))
	}
	return pts
}

type refPoint struct {
	pos uint32
	key string
}

func refLookup(points []refPoint, h uint32) string {
	idx := sort.Search(len(points), func(i int) bool { return points[i].pos >= h })
	if idx == len(points) {
		idx = 0
	}
	return points[idx].key
}

// TestAgainstReference fuzzes the ring against a brute-force implementation
// (smallest position >= hash, wrapping) across replica counts.
func TestAgainstReference(t *testing.T) {
	for _, replicas := range []uint{1, 3, 50, 200} {
		rng := rand.New(rand.NewSource(int64(replicas)))
		ch := New(WithDefaultReplicas(replicas))

		added := map[string]bool{}
		seen := map[uint32]string{}
		collision := false
		var points []refPoint
		for n := 0; n < 500; n++ {
			k := fmt.Sprintf("node-%d", rng.Intn(100000))
			if added[k] {
				continue
			}
			added[k] = true
			ch.Add([]byte(k))
			for _, p := range referencePoints([]byte(k), replicas) {
				if prev, ok := seen[p]; ok && prev != k {
					collision = true
				}
				seen[p] = k
				points = append(points, refPoint{p, k})
			}
		}
		if collision {
			continue // ambiguous tie-breaking; skip this configuration
		}
		sort.Slice(points, func(i, j int) bool { return points[i].pos < points[j].pos })

		for q := 0; q < 5000; q++ {
			lk := fmt.Sprintf("lookup-%d", rng.Intn(1000000))
			got := ch.GetString(lk)
			want := refLookup(points, crc32.ChecksumIEEE([]byte(lk)))
			if got != want {
				t.Fatalf("[rep=%d] Get(%q)=%q want %q", replicas, lk, got, want)
			}
		}
	}
}

// TestAddReplicasRemove ensures a weighted key and all its replicas are fully
// removed (the pre-rewrite code leaked replicas due to a replicaMap key bug).
func TestAddReplicasRemove(t *testing.T) {
	ch := New(WithDefaultReplicas(1))
	ch.Add([]byte("a"), []byte("b"))
	ch.AddReplicas(100, []byte("weighted"))

	before := len(ch.ring)
	if !ch.Remove([]byte("weighted")) {
		t.Fatal("Remove returned false for a present key")
	}
	removed := before - len(ch.ring)
	if removed != 100 {
		t.Fatalf("expected 100 ring positions removed, got %d", removed)
	}
	if got := ch.GetString("weighted"); got == "weighted" {
		t.Fatalf("weighted key still resolves to itself after removal: %q", got)
	}
}

// TestRemoveAbsent verifies Remove reports false for keys that are not present.
func TestRemoveAbsent(t *testing.T) {
	ch := New()
	if ch.Remove([]byte("ghost")) {
		t.Fatal("Remove on empty ring should return false")
	}
	ch.Add([]byte("real"))
	if ch.Remove([]byte("ghost")) {
		t.Fatal("Remove of absent key should return false")
	}
	if !ch.Remove([]byte("real")) {
		t.Fatal("Remove of present key should return true")
	}
}

// TestKeyCopy verifies stored keys are not aliased to the caller's buffer.
func TestKeyCopy(t *testing.T) {
	ch := New(WithDefaultReplicas(1))
	buf := []byte("node-A")
	ch.Add(buf)
	hash := crc32.ChecksumIEEE([]byte("node-A"))
	copy(buf, []byte("xxxxxx")) // mutate caller's buffer after Add
	if got := string(ch.hashMap[hash]); got != "node-A" {
		t.Fatalf("stored key was aliased and mutated: %q", got)
	}
}
