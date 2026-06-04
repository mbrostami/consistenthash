// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"bytes"
	"hash/crc32"
	"sort"
	"sync"
)

// HashFunc hash function to generate random hash
type HashFunc func(data []byte) uint32

type node struct {
	key     uint32 // position on the ring
	pointer uint32 // hash of the original key, used as the index into hashMap
}

// ConsistentHash everything we need for CH
type ConsistentHash struct {
	mu         sync.RWMutex
	hash       HashFunc
	replicas   uint              // default number of replicas in the ring (higher means better balance)
	ring       []node            // all ring positions, kept sorted by node.key
	hashMap    map[uint32][]byte // originalHash -> original key value
	replicaMap map[uint32]uint   // originalHash -> replica count, stored only when it differs from the default
}

// New makes new ConsistentHash
func New(opts ...Option) *ConsistentHash {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	ch := &ConsistentHash{
		replicas:   o.defaultReplicas,
		hash:       o.hashFunc,
		hashMap:    make(map[uint32][]byte),
		replicaMap: make(map[uint32]uint),
	}

	if ch.replicas < 1 {
		ch.replicas = 1
	}

	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
	}

	return ch
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.ring) == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash) Add(keys ...[]byte) {
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash) AddReplicas(replicas uint, keys ...[]byte) {
	if replicas < 1 {
		return
	}
	ch.add(replicas, keys...)
}

// Get finds the closest item in the hash ring to the provided key
func (ch *ConsistentHash) Get(key []byte) []byte {
	hash := ch.hash(key)

	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return nil
	}

	// Binary search for the first ring position >= hash, wrapping to the
	// start of the ring when the key falls past the last position.
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i].key >= hash
	})
	if idx == len(ch.ring) {
		idx = 0
	}

	return ch.hashMap[ch.ring[idx].pointer]
}

// GetString gets the closest item in the hash ring to the provided key
func (ch *ConsistentHash) GetString(key string) string {
	if v := ch.Get([]byte(key)); v != nil {
		return string(v)
	}
	return ""
}

// Remove removes the key (and all of its replicas) from the hash ring.
// It returns false if the key was not present.
func (ch *ConsistentHash) Remove(key []byte) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	originalHash := ch.hash(key)
	if _, ok := ch.hashMap[originalHash]; !ok {
		return false
	}

	replicas := ch.replicas
	if r, found := ch.replicaMap[originalHash]; found {
		replicas = r
	}

	// Collect every ring position this key occupies.
	remove := make(map[uint32]struct{}, replicas)
	for _, h := range ch.replicaHashes(key, replicas) {
		remove[h] = struct{}{}
	}

	// Filter the ring in place, dropping only positions owned by this key.
	w := 0
	for _, n := range ch.ring {
		if _, drop := remove[n.key]; drop && n.pointer == originalHash {
			continue
		}
		ch.ring[w] = n
		w++
	}
	ch.ring = ch.ring[:w]

	delete(ch.hashMap, originalHash)
	delete(ch.replicaMap, originalHash)
	return true
}

// add inserts the given keys, each with "replicas" ring positions.
func (ch *ConsistentHash) add(replicas uint, keys ...[]byte) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for _, key := range keys {
		hashes := ch.replicaHashes(key, replicas)
		originalHash := hashes[0]

		// Store a copy so later mutation of the caller's slice can't change us.
		val := make([]byte, len(key))
		copy(val, key)
		ch.hashMap[originalHash] = val

		for _, h := range hashes {
			ch.ring = append(ch.ring, node{key: h, pointer: originalHash})
		}

		// Only record the replica count when it differs from the default.
		if replicas != ch.replicas {
			ch.replicaMap[originalHash] = replicas
		}
	}

	ch.sortAndDedup()
}

// replicaHashes returns the ring positions for a key: the original hash first,
// then one additional hash per replica. It is deterministic, so Remove can
// recompute exactly the positions Add produced.
func (ch *ConsistentHash) replicaHashes(key []byte, replicas uint) []uint32 {
	hashes := make([]uint32, 0, replicas)
	hashes = append(hashes, ch.hash(key))

	var b bytes.Buffer
	for i := uint32(1); i < uint32(replicas); i++ {
		b.Reset()
		b.Write(key)
		b.WriteByte(byte(i))
		b.WriteByte(byte(i >> 8))
		b.WriteByte(byte(i >> 16))
		b.WriteByte(byte(i >> 24))
		hashes = append(hashes, ch.hash(b.Bytes()))
	}
	return hashes
}

// sortAndDedup keeps the ring sorted by position and drops duplicate positions,
// keeping the first occurrence (so re-adding a key is idempotent).
func (ch *ConsistentHash) sortAndDedup() {
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].key < ch.ring[j].key
	})

	if len(ch.ring) < 2 {
		return
	}

	w := 1
	for i := 1; i < len(ch.ring); i++ {
		if ch.ring[i].key == ch.ring[w-1].key {
			continue // duplicate ring position
		}
		ch.ring[w] = ch.ring[i]
		w++
	}
	ch.ring = ch.ring[:w]
}
