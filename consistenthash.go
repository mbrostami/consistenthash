// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// HashFunc hash function to generate random hash
type HashFunc func(data []byte) uint32

// ConsistentHash everything we need for CH
type ConsistentHash struct {
	sync.RWMutex
	hash     HashFunc
	replicas int               // default number of replicas in hash ring (higher number means more posibility for balance equality)
	hKeys    []uint32          // Sorted
	hTable   map[uint32]string // Hash table unsorted key value pair (hash(x): x) of replicas (nodes)
	rTable   map[string]int    // Number of replicas per stored key
}

// New makes new ConsistentHash
func New(replicas int, hashFunction HashFunc) *ConsistentHash {
	ch := &ConsistentHash{
		replicas: replicas,
		hash:     hashFunction,
		hTable:   make(map[uint32]string),
		rTable:   make(map[string]int),
	}
	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
	}
	return ch
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return len(ch.hKeys) == 0
}

// Add adds some keys to the hash
// key can be also ip:port of a replica
func (ch *ConsistentHash) Add(key string) {
	ch.add(key, ch.replicas)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
// key can be also ip:port of a replica
func (ch *ConsistentHash) AddReplicas(key string, replicas int) {
	if replicas < 1 {
		replicas = ch.replicas
	}
	ch.add(key, replicas)
}

// Get gets the closest item in the hash ring to the provided key
// e.g. key = "request url" O(log n)
func (ch *ConsistentHash) Get(key string) string {
	if ch.IsEmpty() {
		return ""
	}
	ch.RLock()
	defer ch.RUnlock()
	hash := ch.hash([]byte(key))

	// Binary search for appropriate replica
	idx := sort.Search(len(ch.hKeys), func(i int) bool { return ch.hKeys[i] >= hash })

	// Means we have cycled back to the first replica
	if idx == len(ch.hKeys) {
		idx = 0
	}
	return ch.hTable[ch.hKeys[idx]]
}

// Remove removes the key from hash table
func (ch *ConsistentHash) Remove(key string) bool {
	if ch.IsEmpty() {
		return true
	}
	replicas := ch.rTable[key]
	ch.Lock()
	defer ch.Unlock()
	for i := 0; i < replicas; i++ {
		hash := ch.hash([]byte(strconv.Itoa(i) + key))
		delete(ch.hTable, hash) // delete replica
		ch.removeHashKey(hash)
	}
	delete(ch.rTable, key) // delete replica numbers
	return true
}

// removeHashKey remove item from sorted hKeys and keep it sorted O(n)
func (ch *ConsistentHash) removeHashKey(hash uint32) {
	for idx := 0; idx < len(ch.hKeys); idx++ {
		if ch.hKeys[idx] == hash {
			ch.hKeys = append(ch.hKeys[:idx], ch.hKeys[idx+1:]...)
			break
		}
	}
}

// add inserts new hash in hash table
func (ch *ConsistentHash) add(key string, replicas int) {
	ch.Lock()
	defer ch.Unlock()
	for i := 0; i < replicas; i++ {
		hash := ch.hash([]byte(strconv.Itoa(i) + key))
		ch.hKeys = append(ch.hKeys, hash)
		ch.hTable[hash] = key
	}
	ch.rTable[key] = replicas
	// Sort hKeys to speedup lookup
	sort.Slice(ch.hKeys, func(i, j int) bool {
		return ch.hKeys[i] < ch.hKeys[j]
	})
}
