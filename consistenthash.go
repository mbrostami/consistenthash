// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"bytes"
	"hash/crc32"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// HashFunc hash function to generate random hash
type HashFunc func(data []byte) uint32

// ConsistentHash everything we need for CH
type ConsistentHash struct {
	mu                sync.RWMutex
	hash              HashFunc
	replicas          uint              // default number of replicas in hash ring (higher number means more possibility for balance equality)
	hKeys             []uint32          // Sorted
	hTable            map[uint32][]byte // Hash table key value pair (hash(x): x) * replicas (nodes)
	rTable            map[uint32]uint   // Number of replicas per stored key
	blocks            map[uint32]block  // fixed size blocks in the circle each might contain a list of keys
	totalBlocks       uint32
	missed            map[int]int
	stale             *ConsistentHash
	lockState         atomic.Bool
	blockPartitioning int
}

type block struct {
	min uint32
	max uint32
}

// New makes new ConsistentHash
func New(opts ...Option) *ConsistentHash {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	ch := &ConsistentHash{
		replicas: o.defaultReplicas,
		hash:     o.hashFunc,
		hTable:   make(map[uint32][]byte, 0),
		rTable:   make(map[uint32]uint, 0),
	}
	if o.metrics {
		ch.missed = make(map[int]int, 0)
	}

	if ch.replicas < 1 {
		ch.replicas = 1
	}

	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
	}

	ch.blockPartitioning = o.blockPartitioning
	if o.blockPartitioning > 0 {
		ch.blocks = make(map[uint32]block, 0)
	}

	if o.readLockFree {
		ch.stale = &ConsistentHash{
			hash:     ch.hash,
			replicas: ch.replicas,
			hTable:   make(map[uint32][]byte, 0),
			rTable:   make(map[uint32]uint, 0),
		}
	}
	return ch
}

// Metrics return the collected metrics
func (ch *ConsistentHash) Metrics() any {
	return ch.missed
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return len(ch.hKeys) == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash) Add(keys ...[]byte) {
	if ch.stale != nil {
		ch.stale.Add(keys...)
	}
	ch.lock()
	defer ch.unlock()
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash) AddReplicas(replicas uint, keys ...[]byte) {
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

// Get gets the closest item in the hash ring to the provided key as []byte
func (ch *ConsistentHash) Get(key []byte) []byte {
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

	var idx uint32
	// check the first and the last hashes
	if ch.hKeys[len(ch.hKeys)-1] < hash || ch.hKeys[0] > hash {
		idx = 0
	} else if ch.blockPartitioning < 1 {
		// binary search for appropriate replica
		idx = uint32(sort.Search(len(ch.hKeys), func(i int) bool { return ch.hKeys[i] >= hash }))
	} else {
		// block size is equal to hkeys
		// binary search for appropriate replica
		blockSize := math.MaxUint32 / ch.totalBlocks
		blockNumber := hash / blockSize
		var b block
		var ok bool
		var i int
		var j, h uint32
		for blockNumber < ch.totalBlocks {
			b, ok = ch.blocks[blockNumber]
			if !ok {
				blockNumber++
				if ch.missed != nil {
					ch.missed[i]++ // collect number of missed blocks for debugging
					i++
				}
				continue
			}
			idx = b.min
			j = b.max
			// similar to sort.Search (binary search)
			for idx < j {
				h = (idx + j) >> 1
				if h < uint32(len(ch.hKeys)) && ch.hKeys[h] >= hash {
					j = h
				} else {
					idx = h + 1
				}
			}
			break
		}
	}

	// means we have cycled back to the first replica
	if idx > uint32(len(ch.hKeys))-1 {
		idx = 0
	}

	if v, ok := ch.hTable[ch.hKeys[idx]]; ok {
		return v
	}

	return nil
}

// GetString gets the closest item in the hash ring to the provided key
func (ch *ConsistentHash) GetString(key string) string {
	if v := ch.Get([]byte(key)); v != nil {
		return string(v)
	}
	return ""
}

// Remove removes the key from hash table
func (ch *ConsistentHash) Remove(key []byte) bool {
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

	var hash uint32
	var i uint32
	for i = 0; i < uint32(replicas); i++ {
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

// removeHashKey remove item from sorted hKeys and keep it sorted O(n)
func (ch *ConsistentHash) removeHashKey(hash uint32) {
	for i := range ch.hKeys {
		if ch.hKeys[i] == hash {
			ch.hKeys = append(ch.hKeys[:i], ch.hKeys[i+1:]...)
			return
		}
	}
}

// add inserts new hashes in hash table
func (ch *ConsistentHash) add(replicas uint, keys ...[]byte) {
	// increase the capacity of the slice
	n := len(keys) * int(replicas)
	if n -= cap(ch.hKeys) - len(ch.hKeys); n > 0 { // check if we need to grow the slice
		ch.hKeys = append(ch.hKeys[:len(ch.hKeys):len(ch.hKeys)], make([]uint32, n, n)...)[:len(ch.hKeys)]
	}

	var hash uint32
	var i uint32
	var h bytes.Buffer
	for idx := range keys {
		hash = ch.hash(keys[idx])
		ch.hKeys = append(ch.hKeys, hash)
		ch.hTable[hash] = keys[idx][:len(keys[idx]):len(keys[idx])]
		for i = 0; i < uint32(replicas); i++ {
			h.Write(keys[idx])
			h.Write([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
			hash = ch.hash(h.Bytes())
			ch.hKeys = append(ch.hKeys, hash)
			ch.hTable[hash] = keys[idx][:len(keys[idx]):len(keys[idx])]
			h.Reset()
		}

		// do not store number of replicas if uses default number
		if replicas != ch.replicas {
			ch.rTable[hash] = replicas
		}
	}

	// Sort hKeys
	sort.Slice(ch.hKeys, func(i, j int) bool {
		return ch.hKeys[i] < ch.hKeys[j]
	})

	ch.buildBlocks(false)
}

// buildBlocks splits hash table to same size blocks and stores the sorted keys inside specified block
func (ch *ConsistentHash) buildBlocks(rebuild bool) {
	if ch.blockPartitioning < 1 {
		return
	}
	ch.totalBlocks = uint32(len(ch.hKeys) / ch.blockPartitioning)
	if rebuild {
		ch.blocks = make(map[uint32]block, ch.totalBlocks)
	}
	blockSize := math.MaxUint32 / ch.totalBlocks
	var blockNumber uint32
	var ok bool
	var b block
	for idx := range ch.hKeys {
		blockNumber = ch.hKeys[idx] / blockSize
		b, ok = ch.blocks[blockNumber]
		if !ok {
			b = block{
				min: uint32(idx),
			}
		}

		b.max = uint32(idx)
		ch.blocks[blockNumber] = b
	}
}

func (ch *ConsistentHash) lock() {
	ch.mu.Lock()
	ch.lockState.Store(true)
}

func (ch *ConsistentHash) unlock() {
	ch.mu.Unlock()
	ch.lockState.Store(false)
}

func (ch *ConsistentHash) rLock() {
	ch.mu.RLock()
}

func (ch *ConsistentHash) rUnlock() {
	ch.mu.RUnlock()
}
