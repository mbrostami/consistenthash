[![Build Status](https://travis-ci.com/mbrostami/consistenthash.svg?branch=master)](https://travis-ci.com/mbrostami/consistenthash)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing

A Go library that implements Consistent Hashing with zero dependency   
This package is implemented based on [golang/groupcache](https://github.com/golang/groupcache) with some performance improvements

### Improvements:

- `Remove` function added - sort and remove a node (and replicas) from the hash ring
- int hashes replaced with uint32
- Number of replicas is now configurable while adding new node (useful when capacity is not the same for all nodes)

# Technical Details:
A **HashMap** that maps the hash of the key to the original values, something like `0xFF => "node number one"`  

A **BlockMap** that has dynamic number of blocks and each block has sorted list of items, and each item has Key and Pointer to the HashMap. The block number is calculated as:   
`blockNumber = hash(key) / (MaxUint32 / number of blocks)`  
This will lead us to have a kind of sorted blocks.  

A **ReplicaMap** that keeps the number of replicas for each key ONLY if the replicas for specific key is different than the default replicas. So if you have 10k keys with default replica set to 100 and you add a new key with 120 replicas, the ReplicaMap will have 1 record.

### Re-Balancing:
To add a new record we need to know how many blocks we should have. Considering current number of keys and a control option D we will have number of keys divided by D as number of expected blocks.  

As the result will change by adding D number of new keys we will need a re-balancing mechanism to add or remove blocks. To make it efficient we can start re-balancing if the expected blocks is twice more or less than current number of blocks.  

**As an example**:  
If we have D = 200 with 100k existing keys, there will be 1000 blocks each contains approximately 500 keys. Adding 200 new keys will require us to have 1001 blocks. So expected blocks is 1001 but current is 1000. As we add more keys the expected blocks becomes 2001 and current remains 1000. In this case re-balancing process starts and adds 1001 more blocks. As a result we will have 2001 blocks. Next re-balancing will happen when we need 4002 blocks and so on.  

This process happens as follows:  
Looping over blocks from the last one. Looping over items in each block starting from end, calculating the new block number for each key. Because items are sorted, as we reach to a key that doesn't belong to a different block, we shift items to the target block.  
Here is an example:  
```
max hash value = 1000
total keys = 100
D = 5
blockSize = 1000 / (100/5) = 50
blockNumber = hash / blockSize
Number of blocks: 1000 / blockSize = 20
10  / 50 => 0
20  / 50 => 0
50  / 50 => 1
...
1000 / 50 => 20

Block[0]  = [10,20,30,40]
Block[1]  = [50,60,70,80]
Block[4]  = [210,220,230,240]
...

Re-balancing:
newBlockSize = 1000/ (200/5) = 25
newBlockNumber = hash / newBlockSize
Expected blocks: 1000 / newBlockSize = 40
iter b20: // starting from last block
...
iter b4:
Block[0]  = [10,20,30,40]
Block[1]  = [50,60,70,80]
Block[8]  = [210,220]  // moved from block[4]
Block[9]  = [230,240]  // moved from block[4]
itr b1:
Block[0]  = [10,20,30,40]
Block[2]  = [50,60,70] // moved from block[1]
Block[3]  = [80]       // moved from block[1]
Block[8]  = [210,220]  // moved from block[4]
Block[9]  = [230,240]  // moved from block[4]
itr b0:
Block[0]  = [10,20]
Block[1]  = [30,40].   // moved from block[0]
Block[2]  = [50,60,70] // moved from block[1]
Block[3]  = [80]       // moved from block[1]
Block[8]  = [210,220]  // moved from block[4]
Block[9]  = [230,240]  // moved from block[4]
```
Time complexity of the above would be O(n) in worst case where n is the number of keys.  
You can set D value by passing an option to constructor: `WithBlockPartitioning(5)`  

### Adding Item:
Time Complexity is between O(1) and O(log k) where k is the maximum number of keys in a block. (excluding shifting items)

To add a new item to a block, we need to calculate the block number and do a binary search to find the position where we want to insert the item, and update the list in that block. If the item is the original node, we need to add the key and value to the HashMap as well. All the items in the block (including replicas) will use the pointer to the HashMap.  

### Removing Item:
Time Complexity is between O(1) and O(log k) where k is the maximum number of keys in a block. (excluding shifting items) 

To remove an item, we find the block number using hash of the key, doing a binary search in the block, finding removing the item in the block. The re-balancing might be necessary if we remove 1/2 of all the stored keys.  

### Lookup:
Time Complexity is between O(1) and O(log k) where k is the maximum number of keys in a block.  

To find a key, we find the block number using hash of the key, doing a binary search to find the closest hash to the lookup key. You might go to the next non-empty block and get the first item.  

# Usage

`go get github.com/mbrostami/consistenthash/v2`

### Simple use case
```go
package main

import (
	"fmt"

	"github.com/mbrostami/consistenthash/v2"
)

func main() {

	ch := consistenthash.New(consistenthash.WithDefaultReplicas(10))
	ch.Add([]byte("templateA"), []byte("templateB"))

	assignedTemplate := ch.Get([]byte("userA")) // assigned template should always be the same for `userA`
	fmt.Printf("assigned: %s", assignedTemplate)
}
```

### Weighted load


```go
package main

import (
	"fmt"

	"github.com/mbrostami/consistenthash/v2"
)

func main() {
	ch := consistenthash.New(consistenthash.WithDefaultReplicas(10))
	ch.Add([]byte("127.0.0.1:1001"), []byte("127.0.0.1:1002"))
	ch.AddReplicas(40, []byte("127.0.0.1:1003")) // 4x more weight

	rKey := "something like request url or user id"
	node := ch.Get([]byte(rKey)) // find upper closest node
	fmt.Println(node) // this will print out one of the nodes
}

```

## More about consistent hashing  

You can find some explanation in this blog post: https://liuzhenglaichn.gitbook.io/system-design/advanced/consistent-hashing  

The code example to achieve a hash ring similar to the following picture:  
```go
package main

import (
	"fmt"

	"github.com/mbrostami/consistenthash/v2"
)


func main() {
	ch := consistenthash.New(consistenthash.WithDefaultReplicas(3))
	ch.Add([]byte("A"), []byte("B"), []byte("C"))

	fmt.Println(ch.Get([]byte("Alica")))
	fmt.Println(ch.Get([]byte("Bob")))
	fmt.Println(ch.Get([]byte("Casey")))

}

```

![Image](https://1865312850-files.gitbook.io/~/files/v0/b/gitbook-legacy-files/o/assets%2F-M4Bkp-b8HYQgJF1rkOc%2F-M5FwA4YIBAqAjZvdVpU%2F-M5FyQai2CtC2j4GGr5K%2Fimage.png?alt=media&token=77d5d346-f37f-4f28-8f64-66a1627d2deb)
