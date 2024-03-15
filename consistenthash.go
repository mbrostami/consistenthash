// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"bytes"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// HashFunc hash function to generate random hash
type HashFunc func(data []byte) uint32

// ConsistentHash everything we need for CH
type ConsistentHash struct {
	mu        sync.RWMutex
	hash      HashFunc
	replicas  uint              // default number of replicas in hash ring (higher number means more possibility for balance equality)
	hKeys     []uint32          // Sorted
	hTable    map[uint32][]byte // Hash table key value pair (hash(x): x) * replicas (nodes)
	rTable    map[uint32]uint   // Number of replicas per stored key
	blocks    map[uint32]*block // fixed size blocks in the circle each might contain a list of keys
	stale     *ConsistentHash
	lockState atomic.Bool
}

type block struct {
	min  int
	max  int
	keys map[int]uint32
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

	if ch.replicas < 1 {
		ch.replicas = 1
	}

	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
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

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return len(ch.hKeys) == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash) Add(keys ...string) {
	if ch.stale != nil {
		ch.stale.Add(keys...)
	}
	ch.lock()
	defer ch.unlock()
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash) AddReplicas(replicas uint, keys ...string) {
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

// GetBytes gets the closest item in the hash ring to the provided key as []byte
func (ch *ConsistentHash) GetBytes(key []byte) []byte {
	if ch.IsEmpty() {
		return nil
	}

	if ch.stale != nil {
		if ch.lockState.Load() {
			return ch.stale.GetBytes(key)
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

	var idx int
	// check the first and the last hashes
	if ch.hKeys[len(ch.hKeys)-1] < hash || ch.hKeys[0] > hash {
		idx = 0
	} else {
		// block size is equal to hkeys
		// binary search for appropriate replica
		totalKeys := math.MaxUint32 / uint32(len(ch.hKeys))
		blockNumber := hash / totalKeys
		var b *block
		var ok bool
		var j, h int
		for blockNumber < uint32(len(ch.hKeys)) {
			b, ok = ch.blocks[blockNumber]
			if !ok {
				blockNumber++
				continue
			}
			idx = b.min
			j = b.max
			// similar to sort.Search (binary search)
			for idx < j {
				h = int(uint(idx+j) >> 1)
				if !(b.keys[h] >= hash) {
					idx = h + 1
				} else {
					j = h
				}
			}
			break
		}
	}

	// means we have cycled back to the first replica
	if idx == len(ch.hKeys) {
		idx = 0
	}

	if v, ok := ch.hTable[ch.hKeys[idx]]; ok {
		return v
	}

	return nil
}

// Get gets the closest item in the hash ring to the provided key
func (ch *ConsistentHash) Get(key string) string {
	if v := ch.GetBytes([]byte(key)); v != nil {
		return string(v)
	}
	return ""
}

// Remove removes the key from hash table
func (ch *ConsistentHash) Remove(key string) bool {
	if ch.IsEmpty() {
		return true
	}
	if ch.stale != nil {
		ch.stale.Remove(key)
	}
	ch.lock()
	defer ch.unlock()

	originalHash := ch.hash([]byte(key))

	replicas, found := ch.rTable[originalHash]
	if !found {
		// if not found, means using the default number
		replicas = ch.replicas
	}

	var err error
	var b bytes.Buffer
	var hash uint32
	for i := 0; i < int(replicas); i++ {
		if _, err = b.WriteString(key); err != nil {
			return false
		}
		if _, err = b.WriteString(strconv.Itoa(i)); err != nil {
			return false
		}
		hash = ch.hash(b.Bytes())
		delete(ch.hTable, hash) // delete replica
		ch.removeHashKey(hash)
		b.Reset()
	}

	if found {
		delete(ch.rTable, originalHash) // delete replica numbers
	}

	ch.buildBlocks()

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
func (ch *ConsistentHash) add(replicas uint, keys ...string) {
	// increase the capacity of the slice
	n := len(keys) * int(replicas)
	if n -= cap(ch.hKeys) - len(ch.hKeys); n > 0 { // check if we need to grow the slice
		ch.hKeys = append(ch.hKeys[:len(ch.hKeys):len(ch.hKeys)], make([]uint32, n, n)...)[:len(ch.hKeys)]
	}

	var hash uint32
	var i int
	for _, key := range keys {
		var b bytes.Buffer
		var h bytes.Buffer
		for i = 0; i < int(replicas); i++ {
			b.WriteString(key)
			h.WriteString(key)
			if i != 0 { // first item is equal to the key itself
				h.WriteString(strconv.Itoa(i))
			}
			hash = ch.hash(h.Bytes())
			ch.hKeys = append(ch.hKeys, hash)
			ch.hTable[hash] = b.Bytes()[:b.Len():b.Len()]
			b.Reset()
			h.Reset()
		}
		// do not store number of replicas if uses default number
		if replicas != ch.replicas {
			ch.rTable[ch.hash([]byte(key))] = replicas
		}
	}
	// Sort hKeys
	sort.Slice(ch.hKeys, func(i, j int) bool {
		return ch.hKeys[i] < ch.hKeys[j]
	})

	ch.buildBlocks()
}

// buildBlocks splits hash table to same size blocks and stores the sorted keys inside specified block
func (ch *ConsistentHash) buildBlocks() {
	totalKeys := math.MaxUint32 / uint32(len(ch.hKeys))
	ch.blocks = make(map[uint32]*block, len(ch.hKeys)) // maybe sync pool helps here
	var blockNumber uint32
	for idx, key := range ch.hKeys {
		blockNumber = key / totalKeys
		if ch.blocks[blockNumber] == nil {
			ch.blocks[blockNumber] = &block{
				min: idx,
				max: idx,
				keys: map[int]uint32{
					idx: key,
				},
			}
			continue
		}

		ch.blocks[blockNumber].keys[idx] = key
		ch.blocks[blockNumber].max = idx
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
