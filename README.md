[![Build Status](https://travis-ci.com/mbrostami/consistenthash.svg?branch=master)](https://travis-ci.com/mbrostami/consistenthash)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing

A Go library that implements Consistent Hashing with zero dependency   
This package is implemented based on [golang/groupcache](https://github.com/golang/groupcache) with some performance improvements

### Improvements:

- `Remove` function added - sort and remove a node (and replicas) from the hash ring
- int hashes replaced with uint32
- Number of replicas is now configurable while adding new node (useful when capacity is not the same for all nodes)

# Addition to the original algorithm
To make lookups faster, I used the number of registered keys divided by a number (d) in hash ring to create a fixed size of blocks (Block Partitioning) that covers the whole ring.  
Each block consist of zero/multiple sorted keys that are also exist in hash ring, during the lookup, the block number will be calculated with time complexity as O(1).  
Then a binary search will be applied on the keys in that block and index of the key will be returned.   
If the distribution of the hashed keys is roughly uniform, (The more uniform the distribution, the more effective and predictable the performance)       
it means in each block we should expect ~1 key when d = 1, which should end up to: `O(log(n)) >= time complexity >= O(1)` or `O(log(k))` where `k` is the maximum number of elements in the largest block.   
The drawback would be slightly slower writes.       

### Lookup benchmarks:
**WITHOUT** block partitioning:   
```
BenchmarkGet8x50-10                 1000                65.0 ns/op             0 B/op          0 allocs/op
BenchmarkGet512x50-10               1000               164.4 ns/op             0 B/op          0 allocs/op
BenchmarkGet1024x50-10              1000               348.6 ns/op             0 B/op          0 allocs/op
BenchmarkGet4096x50-10              1000               385.4 ns/op             0 B/op          0 allocs/op
BenchmarkGet200000x50-10            1000              1476.0 ns/op             0 B/op          0 allocs/op
```

**WITH** block partitioning with size of total keys / 5:   
```
BenchmarkGet8BPx50-10               1000                33.4 ns/op             0 B/op          0 allocs/op
BenchmarkGet512BPx50-10             1000               103.9 ns/op             0 B/op          0 allocs/op
BenchmarkGet1024BPx50-10            1000               135.2 ns/op             0 B/op          0 allocs/op
BenchmarkGet4096BPx50-10            1000               229.0 ns/op             0 B/op          0 allocs/op
BenchmarkGet200000BPx50-10          1000               343.5 ns/op             0 B/op          0 allocs/op
```

### Block Partitioning Distribution 
#### Explanation
Before running into the experiment consider an example where you have 10 keys:   
If you have 1 block per key, so 10 blocks, means you MIGHT have 1 key in each block but that's not always the case, you may have a block with 2 keys and one with no keys.   
So we need to skip the block that doesn't have any key in it and jump to the next block. Let's call this `Missed blocks`.  

#### Key Distribution (block size):
For 4096x50 = 204k added keys with avg 5 keys in each block we will have around 40k blocks with following distribution:    
```
[1:2578 2:5583 3:7070 4:5692 5:4270 6:3477 7:3291 8:2733 9:2098 10:1578 11:1094 12:629 13:290 14:98 15:30 16:8 17:3]
```
2578 blocks with 1 key  
5583 blocks with 2 keys  
7070 blocks with 3 keys  
5692 blocks with 4 keys  
4270 blocks with 5 keys  
3477 blocks with 6 keys  
3291 blocks with 7 keys  
2733 blocks with 8 keys  
...  
3 blocks with 17 keys  


#### Experiment Missed Blocks:  
Let's have 10M keys added to the ring hash with `crc32.ChecksumIEEE` as hash function. I used `WithMetrics()` to collect the metrics for missed blocks.  

1. To have 1 block per key (10M blocks) I used `WithBlockPartitioning(1)` option.
   Here is the missed blocks for 10k random lookups:  
    ```
    [0:3746 1:1503 2:753 3:437 4:230 5:106 6:50 7:24 8:10 9:6 10:3 11:1 12:1]
    
    Where:
     
    3746 lookups jumped to the second block which:
    1503 of those lookups jumped to the third block which:
    753  of those lookups jumped to the fourth block which:
    ...
    1 of those lookups jumped to 13th block
    ```     

2. To have 1 block for each 5 keys (2M blocks) I used `WithBlockPartitioning(5)` option.
   Here is the missed blocks for 10k random lookups:
    ```
    [0:236 1:5]
    
    Where:
     
    236 lookups jumped to the second block which: 
    5 of those lookups jumped to the third block
    ```     
3. To have 1 block for each 10 keys (1M blocks) I used `WithBlockPartitioning(10)` option.
   Here is the missed blocks for 10k random lookups:
    ```
    [0:5]
    
    Where:
     
    5 lookups jumped to the second block
    ```     


# Benchmark
Each numbers in front of the benchmark name specifies how many keys (x50 replicas) will be added to the ring, so 4096 means (4096 * 50) keys.  
`BenchmarkGet8x50` adds 8*50 keys to the ring, with no block partitioning.  
`BenchmarkGet8BPx50` adds 8*50 keys to the ring, with block partitioning with size of: (total keys / 5)
```bash
> go test ./... -run none -bench Benchmark -benchtime 1000x -benchmem                                                                                                                                                                                                                                                                                                                                                        ─╯
goos: darwin
goarch: arm64
pkg: github.com/mbrostami/consistenthash/v2

BenchmarkGet8x50-10                 1000                65.0 ns/op             0 B/op          0 allocs/op
BenchmarkGet512x50-10               1000               164.4 ns/op             0 B/op          0 allocs/op
BenchmarkGet1024x50-10              1000               348.6 ns/op             0 B/op          0 allocs/op
BenchmarkGet4096x50-10              1000               385.4 ns/op             0 B/op          0 allocs/op
BenchmarkGet200000x50-10            1000             23234.0 ns/op             0 B/op          0 allocs/op
BenchmarkAdd8x50-10                 1000          26563219.0 ns/op         78102 B/op         16 allocs/op
BenchmarkRemove128x50-10            1000             10888.0 ns/op          3200 B/op         50 allocs/op

BenchmarkGet8BPx50-10               1000                33.4 ns/op             0 B/op          0 allocs/op
BenchmarkGet512BPx50-10             1000               103.9 ns/op             0 B/op          0 allocs/op
BenchmarkGet1024BPx50-10            1000               135.2 ns/op             0 B/op          0 allocs/op
BenchmarkGet4096BPx50-10            1000               229.0 ns/op             0 B/op          0 allocs/op
BenchmarkGet200000BPx50-10          1000               343.5 ns/op             0 B/op          0 allocs/op
BenchmarkAdd8BPx50-10               1000          29747129.0 ns/op         82258 B/op         18 allocs/op
BenchmarkRemove128BPx50-10          1000             22609.0 ns/op          6736 B/op         54 allocs/op


```
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
	ch.Add("templateA", "templateB")

	assignedTemplate := ch.Get("userA") // assigned template should always be the same for `userA`
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
	ch.Add("127.0.0.1:1001", "127.0.0.1:1002")
	ch.AddReplicas(40, "127.0.0.1:1003") // 4x more weight

	rKey := "something like request url or user id"
	node := ch.Get(rKey) // find upper closest node
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
	ch.Add("A", "B", "C")

	fmt.Println(ch.Get("Alica")) 
	fmt.Println(ch.Get("Bob")) 
	fmt.Println(ch.Get("Casey")) 
	
}

```

![Image](https://1865312850-files.gitbook.io/~/files/v0/b/gitbook-legacy-files/o/assets%2F-M4Bkp-b8HYQgJF1rkOc%2F-M5FwA4YIBAqAjZvdVpU%2F-M5FyQai2CtC2j4GGr5K%2Fimage.png?alt=media&token=77d5d346-f37f-4f28-8f64-66a1627d2deb)
