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
	metricsmu         sync.Mutex
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
	ch.metricsmu.Lock()
	defer ch.metricsmu.Unlock()

	// block size distribution
	ch.metrics["blockSize"] = make(map[int]int, 0)
	for _, nodes := range ch.blocks {
		ch.metrics["blockSize"][len(nodes)]++
	}
	ch.metrics["totalKeys"] = map[int]int{0: int(ch.totalKeys)}
	ch.metrics["totalBlocks"] = map[int]int{0: int(ch.totalBlocks)}
	return ch.metrics
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return ch.totalKeys == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash) Add(keys ...[]byte) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash) AddReplicas(replicas uint, keys ...[]byte) {
	if replicas < 1 {
		return
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.add(replicas, keys...)
}

// Get finds the closest item in the hash ring to the provided key
func (ch *ConsistentHash) Get(key []byte) []byte {
	if ch.IsEmpty() {
		return nil
	}

	hash := ch.hash(key)

	ch.mu.RLock()
	defer ch.mu.RUnlock()

	// check if the exact match exist in the hash table
	if v, ok := ch.hTable[hash]; ok {
		return v
	}

	v, _ := ch.lookup(hash)
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

	originalHash := ch.hash(key)

	ch.mu.Lock()
	defer ch.mu.Unlock()

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

	expectedBlocks := (ch.totalKeys + uint32(len(nodes))) / ch.blockPartitioning
	ch.balanceBlocks(expectedBlocks)
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
		return nodes[i].key >= n.key
	})

	// check for duplication, ignore if it's duplicate
	if idx < len(nodes) && nodes[idx].key == n.key {
		return
	}

	ch.blocks[blockNumber] = append(
		ch.blocks[blockNumber][:idx],
		append([]node{n}, ch.blocks[blockNumber][idx:]...)...,
	)
	ch.totalKeys++
}

// balanceBlocks checks all the keys in each block and shifts to the next block if the number of blocks needs to be changed
func (ch *ConsistentHash) balanceBlocks(expectedBlocks uint32) {
	// re-balance the blocks if expectedBlocks needs twice size as it's current size
	if (expectedBlocks >> 1) > ch.totalBlocks {
		ch.metricRebalanced(expectedBlocks)
		blockSize := math.MaxUint32 / expectedBlocks
		for blockNumber := uint32(0); blockNumber < expectedBlocks-1; blockNumber++ {
			nodes := ch.blocks[blockNumber]
			targetBlock := blockNumber
			var j int
			for i := 0; i < len(nodes); i++ {
				// the first item not in the block, so the next items will not be in the block as well
				if nodes[i].key/blockSize == targetBlock {
					continue
				}
				if targetBlock == blockNumber {
					// update current block items
					ch.blocks[blockNumber] = nodes[:i][:i:i]
				}

				targetBlock++
				for j = i; j < len(nodes); j++ {
					if nodes[j].key/blockSize != targetBlock {
						break
					}
				}
				if i != j {
					ch.blocks[targetBlock] = append(nodes[i:j], ch.blocks[targetBlock]...)
					i = j
				}
			}
		}

		ch.totalBlocks = expectedBlocks
	} else if expectedBlocks < (ch.totalBlocks >> 1) {
		// TODO decrease number of blocks to avoid missed blocks
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

	// TODO efficient way would be to use ch.lookup(hash uint32)
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

// lookup finds the block number and value of the given hash
func (ch *ConsistentHash) lookup(hash uint32) ([]byte, uint32) {
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
				ch.metricMissed(i)
				i++
			}
			continue
		}
		// binary search inside the block
		idx = sort.Search(len(nodes), func(i int) bool {
			return nodes[i].key >= hash
		})

		// if not found in the block, the first item from the next block is the answer
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

	// if we reach the last block, we need to find the first block that has an item
	if blockNumber == ch.totalBlocks {
		var j uint32
		for j < uint32(len(ch.blocks)) {
			if len(ch.blocks[j]) > 0 {
				blockNumber = 0
				firstKey := ch.blocks[0][0].pointer
				return ch.hTable[firstKey], blockNumber
			}
			if ch.metrics != nil {
				ch.metricMissed(int(j))
			}
			j++
		}
	}
	return nil, blockNumber
}

// metricMissed collect number of missed blocks for debugging
func (ch *ConsistentHash) metricMissed(i int) {
	if ch.metrics == nil {
		return
	}

	ch.metricsmu.Lock()
	defer ch.metricsmu.Unlock()

	if ch.metrics["missed"] == nil {
		ch.metrics["missed"] = make(map[int]int, 0)
	}
	ch.metrics["missed"][i]++
}

// metricRebalanced collect number of re-balancing blocks for debugging
func (ch *ConsistentHash) metricRebalanced(expectedBlocks uint32) {
	if ch.metrics == nil {
		return
	}

	ch.metricsmu.Lock()
	defer ch.metricsmu.Unlock()
	if ch.metrics["re-balance"] == nil {
		ch.metrics["re-balance"] = make(map[int]int, 0)
	}
	ch.metrics["re-balance"][len(ch.metrics["re-balance"])] = int(expectedBlocks)
}
