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
	pool              sync.Pool
	replicas          uint              // default number of replicas in hash ring (higher number means more possibility for balance equality)
	hashMap           map[uint32][]byte // Hash table key value pair (hash(x): x) * replicas (nodes)
	replicaMap        map[uint32]uint   // Number of replicas per stored key
	blockMap          map[uint32][]node // fixed size blocks in the circle each might contain a list of keys
	totalBlocks       uint32
	totalKeys         uint32
	blockPartitioning uint32
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
		hashMap:    make(map[uint32][]byte, 0),
		replicaMap: make(map[uint32]uint, 0),
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
	ch.blockMap = make(map[uint32][]node, ch.blockPartitioning)
	ch.pool = sync.Pool{New: func() any { return make(map[uint32][]node, o.blockPartitioning) }}
	ch.totalBlocks = 1

	return ch
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return ch.totalKeys == 0
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
	if ch.IsEmpty() {
		return nil
	}

	hash := ch.hash(key)

	ch.mu.RLock()
	defer ch.mu.RUnlock()

	// check if the exact match exist in the hash table
	if v, ok := ch.hashMap[hash]; ok {
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

	ch.mu.RLock()
	if _, ok := ch.hashMap[originalHash]; !ok {
		ch.mu.RUnlock()
		return false
	}

	replicas, found := ch.replicaMap[originalHash]
	if !found {
		// if not found, means using the default number
		replicas = ch.replicas
	}
	ch.mu.RUnlock()

	nodes := make([]node, replicas, replicas) // todo avoid overflow

	nodes[0] = node{originalHash, originalHash}
	var hash uint32
	var i uint32

	for i = 1; i < uint32(replicas); i++ {
		var b bytes.Buffer
		b.Write(key)
		b.Write([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		hash = ch.hash(b.Bytes())
		nodes[i] = node{hash, originalHash}
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if found {
		delete(ch.replicaMap, originalHash) // delete replica numbers
	}
	for _, n := range nodes {
		ch.remove(n.key, n.pointer)
	}

	expectedBlocks := ch.totalKeys / ch.blockPartitioning
	if expectedBlocks > 0 {
		ch.balanceBlocks(expectedBlocks)
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
		ch.mu.Lock()
		ch.hashMap[originalHash] = keys[idx][:len(keys[idx]):len(keys[idx])]
		ch.mu.Unlock()
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
			ch.mu.Lock()
			ch.replicaMap[hash] = replicas
			ch.mu.Unlock()
		}
	}
	ch.addNodes(nodes)
}

func (ch *ConsistentHash) addNodes(nodes []node) {
	expectedBlocks := (ch.totalKeys + uint32(len(nodes))) / ch.blockPartitioning
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.balanceBlocks(expectedBlocks)
	for i := range nodes {
		ch.addNode(nodes[i])
	}
}

func (ch *ConsistentHash) addNode(n node) {
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := n.key / blockSize
	nodes, ok := ch.blockMap[blockNumber]
	if !ok {
		ch.blockMap[blockNumber] = []node{n}
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

	ch.blockMap[blockNumber] = append(ch.blockMap[blockNumber], node{})
	copy(ch.blockMap[blockNumber][idx+1:], ch.blockMap[blockNumber][idx:])
	ch.blockMap[blockNumber][idx] = n
	ch.totalKeys++
}

// balanceBlocks checks all the keys in each block and shifts to the next block if the number of blocks needs to be changed
func (ch *ConsistentHash) balanceBlocks(expectedBlocks uint32) {
	if expectedBlocks < 1 {
		return
	}
	// re-balance the blocks if expectedBlocks needs twice size as it's current size
	if (expectedBlocks>>1) > ch.totalBlocks || expectedBlocks < (ch.totalBlocks>>1) {
		blockSize := math.MaxUint32 / expectedBlocks
		newBlockMap := ch.pool.Get().(map[uint32][]node)
		for blockNumber := ch.totalBlocks; blockNumber >= 0; blockNumber-- {
			nodes := ch.blockMap[blockNumber]
			var j int
			for i := len(nodes) - 1; i > 0; i-- {
				targetBlock := nodes[i].key / blockSize
				if targetBlock == blockNumber {
					newBlockMap[blockNumber] = nodes[:i]
					break
				}
				for j = i; j > 0; j-- {
					if nodes[j].key/blockSize != targetBlock {
						break
					}
				}
				// shift and prepend nodes to the target block
				newBlockMap[targetBlock] = append(newBlockMap[targetBlock], make([]node, i-j)...)
				copy(newBlockMap[targetBlock][i-j:], newBlockMap[targetBlock])
				copy(newBlockMap[targetBlock][:i-j], nodes[j:i-1])
				// newBlockMap[targetBlock] = append(nodes[j:i-1], newBlockMap[targetBlock]...)

				i = j
			}
			if blockNumber == 0 {
				break
			}
		}
		ch.blockMap = newBlockMap
		ch.totalBlocks = expectedBlocks
		ch.pool.Put(newBlockMap)
	}

	if ch.totalBlocks < 1 {
		ch.totalBlocks = 1
	}
}

// remove removes one key from a block
func (ch *ConsistentHash) remove(hash, originalHash uint32) {
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	nodes, ok := ch.blockMap[blockNumber]
	if !ok {
		return
	}

	ln := len(nodes)
	idx := sort.Search(ln, func(i int) bool {
		return nodes[i].key >= hash
	})
	if idx == ln {
		ch.blockMap[blockNumber] = ch.blockMap[blockNumber][:idx]
	} else {
		ch.blockMap[blockNumber] = append(ch.blockMap[blockNumber][:idx], ch.blockMap[blockNumber][idx+1:]...) // remove item
	}
	ch.totalKeys--

	if originalHash == hash {
		delete(ch.hashMap, originalHash)
	}
	return
}

// lookup finds the block number and value of the given hash
func (ch *ConsistentHash) lookup(hash uint32) ([]byte, uint32) {
	// binary search for appropriate replica
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	var idx int
	var fullCircle bool
	for blockNumber < ch.totalBlocks {
		nodes, ok := ch.blockMap[blockNumber]
		if !ok {
			blockNumber++
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
			} else {
				blockNumber++
			}
			continue
		}

		// lookup the pointer in hash table
		return ch.hashMap[nodes[idx].pointer], blockNumber
	}

	// if we reach the last block, we need to find the first block that has an item
	if blockNumber == ch.totalBlocks {
		var j uint32
		for j < uint32(len(ch.blockMap)) {
			if len(ch.blockMap[j]) > 0 {
				blockNumber = 0
				firstKey := ch.blockMap[0][0].pointer
				return ch.hashMap[firstKey], blockNumber
			}
			j++
		}
	}
	return nil, blockNumber
}
