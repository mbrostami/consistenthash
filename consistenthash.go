// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"bytes"
	"hash/crc32"
	"math"
	"sort"
	"sync"
)

// HashFunc hash function to generate random hash
type HashFunc func(data []byte) uint32

type node struct {
	key     uint32
	pointer uint32
}

// ConsistentHash everything we need for CH
type ConsistentHash struct {
	mu                sync.RWMutex
	hash              HashFunc
	replicas          uint              // default number of replicas in hash ring (higher number means more possibility for balance equality)
	hTable            map[uint32][]byte // Hash table key value pair (hash(x): x) * replicas (nodes)
	rTable            map[uint32]uint   // Number of replicas per stored key
	blocks            map[uint32][]node // fixed size blocks in the circle each might contain a list of min/max ids
	totalBlocks       uint32
	totalKeys         uint32
	metrics           map[string]map[int]int
	blockPartitioning uint32
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
		ch.metrics = make(map[string]map[int]int, 0)
	}

	if ch.replicas < 1 {
		ch.replicas = 1
	}

	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
	}

	if o.blockPartitioning < 1 {
		o.blockPartitioning = 1
	}

	ch.blockPartitioning = uint32(o.blockPartitioning)
	ch.blocks = make(map[uint32][]node, ch.blockPartitioning)
	ch.totalBlocks = 1

	return ch
}

// Metrics return the collected metrics
func (ch *ConsistentHash) Metrics() any {
	// block size distribution
	ch.metrics["blockSize"] = make(map[int]int, 0)
	for _, nodes := range ch.blocks {
		ch.metrics["blockSize"][len(nodes)]++
	}
	return ch.metrics
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return ch.totalKeys == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash) Add(keys ...[]byte) {
	ch.lock()
	defer ch.unlock()
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash) AddReplicas(replicas uint, keys ...[]byte) {
	if replicas < 1 {
		return
	}
	ch.lock()
	defer ch.unlock()
	ch.add(replicas, keys...)
}

// Get finds the closest item in the hash ring to the provided key
func (ch *ConsistentHash) Get(key []byte) []byte {
	if ch.IsEmpty() {
		return nil
	}

	ch.rLock()
	defer ch.rUnlock()

	hash := ch.hash(key)

	// check if the exact match exist in the hash table
	if v, ok := ch.hTable[hash]; ok {
		return v
	}

	v, _ := ch.lookupFromBlock(hash)
	return v
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
	ch.lock()
	defer ch.unlock()

	originalHash := ch.hash(key)

	replicas, found := ch.rTable[originalHash]
	if !found {
		// if not found, means using the default number
		replicas = ch.replicas
	}

	// remove original one
	ch.removeFromBlock(originalHash, originalHash)

	var hash uint32
	var i uint32
	// remove replicas
	for i = 1; i < uint32(replicas); i++ {
		var b bytes.Buffer
		b.Write(key)
		b.Write([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		hash = ch.hash(b.Bytes())
		ch.removeFromBlock(hash, originalHash)
	}
	if found {
		delete(ch.rTable, originalHash) // delete replica numbers
	}

	return true
}

// add inserts new hashes in hash table
func (ch *ConsistentHash) add(replicas uint, keys ...[]byte) {
	var hash uint32
	var i uint32
	var h bytes.Buffer
	nodes := make([]node, 0, uint(len(keys))*replicas) // todo avoid overflow
	for idx := range keys {
		originalHash := ch.hash(keys[idx])
		// no need for extra capacity, just get the bytes we need
		ch.hTable[originalHash] = keys[idx][:len(keys[idx]):len(keys[idx])]
		nodes = append(nodes, node{originalHash, originalHash})
		for i = 1; i < uint32(replicas); i++ {
			h.Write(keys[idx])
			h.WriteByte(byte(i))
			h.WriteByte(byte(i >> 8))
			h.WriteByte(byte(i >> 16))
			h.WriteByte(byte(i >> 24))
			hash = ch.hash(h.Bytes())
			h.Reset()
			nodes = append(nodes, node{hash, originalHash})
		}

		// do not store number of replicas if uses default number
		if replicas != ch.replicas {
			ch.rTable[hash] = replicas
		}
	}
	ch.addNodes(nodes)
}

func (ch *ConsistentHash) addNodes(nodes []node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].key < nodes[j].key
	})

	totalBlocks := (ch.totalKeys + uint32(len(nodes))) / ch.blockPartitioning
	ch.balanceBlocks(totalBlocks)
	for i := range nodes {
		ch.addNode(nodes[i])
	}
}

func (ch *ConsistentHash) addNode(n node) {
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := n.key / blockSize
	nodes, ok := ch.blocks[blockNumber]
	if !ok {
		ch.blocks[blockNumber] = []node{n}
		ch.totalKeys++
		return
	}
	idx := sort.Search(len(nodes), func(i int) bool {
		return nodes[i].key > n.key
	})
	ch.blocks[blockNumber] = append(
		ch.blocks[blockNumber][:idx],
		append([]node{n}, ch.blocks[blockNumber][idx:]...)...,
	)
	ch.totalKeys++
}

// balanceBlocks checks all the keys in each block and move those to the next block if the number of blocks needs to be changed
func (ch *ConsistentHash) balanceBlocks(totalBlocks uint32) {
	// increasing the number of blocks
	if totalBlocks > ch.totalBlocks {
		blockSize := math.MaxUint32 / totalBlocks
		//offset := 0
		for blockNumber := uint32(0); blockNumber < totalBlocks-1; blockNumber++ {
			for i := 0; i < len(ch.blocks[blockNumber]); i++ {
				// the first item not in the block so the next items will not be in the block as well
				if ch.blocks[blockNumber][i].key/blockSize != blockNumber {
					// prepend items to the next block
					ch.blocks[blockNumber+1] = append(ch.blocks[blockNumber][i:], ch.blocks[blockNumber+1]...)
					// remove items from current block
					ch.blocks[blockNumber] = ch.blocks[blockNumber][:i][:len(ch.blocks[blockNumber][:i]):len(ch.blocks[blockNumber][:i])]
					break
				}
			}
		}

		ch.totalBlocks = totalBlocks
	} else if totalBlocks < ch.totalBlocks {
		// TODO decrease number of blocks
	}

	if ch.totalBlocks < 1 {
		ch.totalBlocks = 1
	}
}

// removeFromBlock removes one key from a block
func (ch *ConsistentHash) removeFromBlock(hash, originalHash uint32) {
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	_, ok := ch.blocks[blockNumber]
	if !ok {
		return
	}

	// TODO efficient way would be too use ch.lookupFromBlock(hash uint32)
	for i := range ch.blocks[blockNumber] {
		if ch.blocks[blockNumber][i].key == hash {
			ch.blocks[blockNumber] = append(ch.blocks[blockNumber][:i], ch.blocks[blockNumber][i+1:]...) // remove item
			ch.totalKeys--
			break
		}
	}
	if originalHash == hash {
		delete(ch.hTable, originalHash)
	}
	return
}

// lookupFromBlock finds the block number and index of the given hash
func (ch *ConsistentHash) lookupFromBlock(hash uint32) ([]byte, uint32) {
	// block size is equal to hkeys
	// binary search for appropriate replica
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	var idx, i int
	var fullCircle bool
	for blockNumber < ch.totalBlocks {
		nodes, ok := ch.blocks[blockNumber]
		if !ok {
			blockNumber++
			if ch.metrics != nil {
				ch.storeMissed(i)
				i++
			}
			continue
		}
		idx = sort.Search(len(nodes), func(i int) bool {
			return nodes[i].key >= hash
		})

		if idx == len(nodes) {
			if blockNumber == ch.totalBlocks-1 && !fullCircle {
				// go to the first block
				blockNumber = 0
				fullCircle = true
			}
			blockNumber++
			continue
		}

		// lookup the pointer in hash table
		return ch.hTable[nodes[idx].pointer], blockNumber
	}

	if blockNumber == ch.totalBlocks && len(ch.blocks[0]) > 0 {
		blockNumber = 0
		firstKey := ch.blocks[0][0].pointer
		return ch.hTable[firstKey], blockNumber
	}
	return nil, blockNumber
}

// storeMissed collect number of missed blocks for debugging
func (ch *ConsistentHash) storeMissed(i int) {
	if ch.metrics == nil {
		return
	}
	if ch.metrics["missed"] == nil {
		ch.metrics["missed"] = make(map[int]int, 0)
	}
	ch.metrics["missed"][i]++
}

func (ch *ConsistentHash) lock() {
	ch.mu.Lock()
}

func (ch *ConsistentHash) unlock() {
	ch.mu.Unlock()
}

func (ch *ConsistentHash) rLock() {
	ch.mu.RLock()
}

func (ch *ConsistentHash) rUnlock() {
	ch.mu.RUnlock()
}
