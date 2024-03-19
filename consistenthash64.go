// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"bytes"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
)

// HashFunc64 hash function to generate random hash
type HashFunc64 func(data []byte) uint64

// ConsistentHash64 everything we need for CH
type ConsistentHash64 struct {
	mu                sync.RWMutex
	hash              HashFunc64
	replicas          uint                 // default number of replicas in hash ring (higher number means more possibility for balance equality)
	hKeys             []uint64             // Sorted
	hTable            map[uint64][]byte    // Hash table key value pair (hash(x): x) * replicas (nodes)
	rTable            map[uint64]uint      // Number of replicas per stored key
	blocks            map[uint64][2]uint64 // fixed size blocks in the circle each might contain a list of min/max ids
	totalBlocks       uint64
	metrics           map[string]map[int]int
	stale             *ConsistentHash64
	lockState         atomic.Bool
	blockPartitioning int
}

// New64 makes new ConsistentHash
func New64(opts ...Option) *ConsistentHash64 {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	ch := &ConsistentHash64{
		replicas: o.defaultReplicas,
		hash:     o.hashFunc64,
		hTable:   make(map[uint64][]byte, 0),
		rTable:   make(map[uint64]uint, 0),
	}
	if o.metrics {
		ch.metrics = make(map[string]map[int]int, 0)
	}

	if ch.replicas < 1 {
		ch.replicas = 1
	}

	if ch.hash == nil {
		ch.hash = func(data []byte) uint64 {
			return xxhash.Sum64(data)
		}
	}

	ch.blockPartitioning = o.blockPartitioning
	if o.blockPartitioning > 0 {
		ch.blocks = make(map[uint64][2]uint64, 0)
	}

	if o.readLockFree {
		ch.stale = &ConsistentHash64{
			hash:     ch.hash,
			replicas: ch.replicas,
			hTable:   make(map[uint64][]byte, 0),
			rTable:   make(map[uint64]uint, 0),
		}
	}
	return ch
}

// Metrics return the collected metrics
func (ch *ConsistentHash64) Metrics() any {
	// block size distribution
	if ch.blockPartitioning > 0 {
		ch.metrics["blockSize"] = make(map[int]int, 0)
		for _, uint64s := range ch.blocks {
			if uint64s[1] == 0 && uint64s[0] == 0 {
				ch.metrics["blockSize"][0]++
			} else {
				ch.metrics["blockSize"][int(uint64s[1]-uint64s[0])+1]++
			}
		}
	}
	return ch.metrics
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash64) IsEmpty() bool {
	return len(ch.hKeys) == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash64) Add(keys ...[]byte) {
	if ch.stale != nil {
		ch.stale.Add(keys...)
	}
	ch.lock()
	defer ch.unlock()
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash64) AddReplicas(replicas uint, keys ...[]byte) {
	if replicas < 1 {
		return
	}
	if ch.stale != nil {
		ch.stale.AddReplicas(replicas, keys...)
	}
	ch.lock()
	defer ch.unlock()
	ch.add(replicas, keys...)
}

// Get finds the closest item in the hash ring to the provided key
func (ch *ConsistentHash64) Get(key []byte) []byte {
	if ch.IsEmpty() {
		return nil
	}

	if ch.stale != nil {
		if ch.lockState.Load() {
			return ch.stale.Get(key)
		}
	} else {
		// stale mode
		// use mutex read lock
		ch.rLock()
		defer ch.rUnlock()
	}

	hash := ch.hash(key)

	// check if the exact match exist in the hash table
	if v, ok := ch.hTable[hash]; ok {
		return v
	}

	var idx uint64
	// check the first and the last hashes
	if ch.hKeys[len(ch.hKeys)-1] < hash || ch.hKeys[0] > hash {
		idx = 0
	} else if ch.blockPartitioning < 1 {
		// binary search for appropriate replica
		idx = uint64(sort.Search(len(ch.hKeys), func(i int) bool { return ch.hKeys[i] >= hash }))
	} else {
		idx, _ = ch.lookupFromBlock(hash)
	}

	// means we have cycled back to the first replica
	if idx > uint64(len(ch.hKeys))-1 {
		idx = 0
	}

	if v, ok := ch.hTable[ch.hKeys[idx]]; ok {
		return v
	}

	return nil
}

// GetString gets the closest item in the hash ring to the provided key
func (ch *ConsistentHash64) GetString(key string) string {
	if v := ch.Get([]byte(key)); v != nil {
		return string(v)
	}
	return ""
}

// Remove removes the key from hash table
func (ch *ConsistentHash64) Remove(key []byte) bool {
	if ch.IsEmpty() {
		return true
	}
	if ch.stale != nil {
		ch.stale.Remove(key)
	}
	ch.lock()
	defer ch.unlock()

	originalHash := ch.hash(key)

	replicas, found := ch.rTable[originalHash]
	if !found {
		// if not found, means using the default number
		replicas = ch.replicas
	}

	var hash uint64
	var i uint64
	for i = 1; i < uint64(replicas); i++ {
		var b bytes.Buffer
		b.Write(key)
		b.Write([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		hash = ch.hash(b.Bytes())
		delete(ch.hTable, hash) // delete replica
		ch.removeHashKey(hash)
	}

	if found {
		delete(ch.rTable, originalHash) // delete replica numbers
	}

	// TODO
	ch.buildBlocks(true)

	return true
}

// removeHashKey remove item from sorted hKeys and keep it sorted
func (ch *ConsistentHash64) removeHashKey(hash uint64) {
	idx := sort.Search(len(ch.hKeys), func(i int) bool { return ch.hKeys[i] >= hash })
	if idx == len(ch.hKeys) {
		return
	}
	if ch.hKeys[idx] == hash {
		ch.hKeys = append(ch.hKeys[:idx], ch.hKeys[idx+1:]...)
	}
}

// add inserts new hashes in hash table
func (ch *ConsistentHash64) add(replicas uint, keys ...[]byte) {
	// increase the capacity of the slice
	n := len(keys) * int(replicas)
	if n -= cap(ch.hKeys) - len(ch.hKeys); n > 0 { // check if we need to grow the slice
		ch.hKeys = append(ch.hKeys[:len(ch.hKeys):len(ch.hKeys)], make([]uint64, n, n)...)[:len(ch.hKeys)]
	}

	var hash uint64
	var i uint64
	var h bytes.Buffer
	for idx := range keys {
		hash = ch.hash(keys[idx])
		ch.hKeys = append(ch.hKeys, hash)
		// no need for extra capacity, just get the bytes we need
		ch.hTable[hash] = keys[idx][:len(keys[idx]):len(keys[idx])]
		for i = 1; i < uint64(replicas); i++ {
			h.Write(keys[idx])
			h.WriteByte(byte(i))
			h.WriteByte(byte(i >> 8))
			h.WriteByte(byte(i >> 16))
			h.WriteByte(byte(i >> 24))
			h.WriteByte(byte(i >> 32))
			h.WriteByte(byte(i >> 40))
			h.WriteByte(byte(i >> 48))
			h.WriteByte(byte(i >> 56))
			hash = ch.hash(h.Bytes())
			ch.hKeys = append(ch.hKeys, hash)
			// no need for extra capacity, just get the bytes we need
			ch.hTable[hash] = keys[idx][:len(keys[idx]):len(keys[idx])]
			h.Reset()
		}

		// do not store number of replicas if uses default number
		if replicas != ch.replicas {
			ch.rTable[hash] = replicas
		}
	}

	// Sort hKeys
	SortUints64(ch.hKeys)

	ch.buildBlocks(false)
}

// buildBlocks splits hash table to same size blocks and stores the sorted keys inside specified block
func (ch *ConsistentHash64) buildBlocks(rebuild bool) {
	if ch.blockPartitioning < 1 {
		return
	}
	ch.totalBlocks = uint64(len(ch.hKeys) / ch.blockPartitioning)
	if rebuild {
		ch.blocks = make(map[uint64][2]uint64, ch.totalBlocks)
	}
	blockSize := math.MaxUint32 / ch.totalBlocks
	var blockNumber uint64
	var ok bool
	var b [2]uint64
	for idx := range ch.hKeys {
		blockNumber = ch.hKeys[idx] / blockSize
		b, ok = ch.blocks[blockNumber]
		if !ok {
			b[0] = uint64(idx) // min
		}
		b[1] = uint64(idx) // max
		ch.blocks[blockNumber] = b
	}
}

// lookupFromBlock finds the block number and index of the given hash
func (ch *ConsistentHash64) lookupFromBlock(hash uint64) (uint64, uint64) {
	// block size is equal to hkeys
	// binary search for appropriate replica
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	var b [2]uint64
	var ok bool
	var i int
	var idx, j, h uint64
	for blockNumber < ch.totalBlocks {
		b, ok = ch.blocks[blockNumber]
		if !ok {
			blockNumber++
			if ch.metrics != nil {
				ch.storeMissed(i)
				i++
			}
			continue
		}
		idx = b[0] // min
		j = b[1]   // max
		// similar to sort.Search (binary search)
		for idx < j {
			h = (idx + j) >> 1
			if h < uint64(len(ch.hKeys)) && ch.hKeys[h] >= hash {
				j = h
			} else {
				idx = h + 1
			}
		}
		break
	}
	return idx, blockNumber
}

// storeMissed collect number of missed blocks for debugging
func (ch *ConsistentHash64) storeMissed(i int) {
	if ch.metrics == nil {
		return
	}
	if ch.metrics["missed"] == nil {
		ch.metrics["missed"] = make(map[int]int, 0)
	}
	ch.metrics["missed"][i]++
}

func (ch *ConsistentHash64) lock() {
	ch.mu.Lock()
	ch.lockState.Store(true)
}

func (ch *ConsistentHash64) unlock() {
	ch.mu.Unlock()
	ch.lockState.Store(false)
}

func (ch *ConsistentHash64) rLock() {
	ch.mu.RLock()
}

func (ch *ConsistentHash64) rUnlock() {
	ch.mu.RUnlock()
}
