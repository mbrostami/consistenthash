[![Build Status](https://travis-ci.com/mbrostami/consistenthash.svg?branch=master)](https://travis-ci.com/mbrostami/consistenthash)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing

A Go library that implements Consistent Hashing with zero dependency   
This package is implemented based on [golang/groupcache](https://github.com/golang/groupcache) with some performance improvements

### Improvements:

- `Remove` function added - sort and remove a node (and replicas) from the hash ring
- int hashes replaced with uint32
- Number of replicas is now configurable while adding new node (This is useful when capacity is not the same for all nodes)

# Addition to the original algorithm
To make lookups faster, I used the number of registered keys in hash ring to create a fixed size of blocks (Block Partitioning) that covers the whole ring.  
Each block consist of zero/multiple sorted keys that are also exist in hash ring, during the lookup, the block number will be calculated with time complexity as O(1).  
Then a binary search will be applied on the keys in that block and index of the key will be returned.   
If the distribution of the hashed keys is roughly uniform, (The more uniform the distribution, the more effective and predictable the performance)       
it means in each block we should expect ~1 key, which should end up to: `O(log(n)) >= time complexity >= O(1)` or `O(log(k))` where `k` is the maximum number of elements in the largest block.   
The drawback would be more memory usage, and slightly slower writes.        


# Addition to the implementation
To have a lock free lookup I added a stale consistent hash, that is a copy of the original one, and will be used when modification is happening to the original one, and there is an active write lock.    
You can enable this feature by passing `WithReadLockFree(true)` as option to the constructor.   

# Benchmark
Each numbers in front of the benchmark name specifies how many keys (*50 replicas) will be added to the ring, so 4096 means (4096 * 50) keys.   
```bash
> go test . -run none -bench Benchmark -benchtime 3s -benchmem                                                                                                                                                                                                                                                                                                                                                               ─╯
goos: darwin
goarch: arm64
pkg: github.com/mbrostami/consistenthash/v2
BenchmarkGetBytes8-10                   82987790                39.08 ns/op            0 B/op          0 allocs/op
BenchmarkGetBytes512-10                 104776002               33.42 ns/op            0 B/op          0 allocs/op
BenchmarkGetBytes1024-10                99181714                34.33 ns/op            0 B/op          0 allocs/op
BenchmarkGetBytes4096-10                87755408                35.34 ns/op            0 B/op          0 allocs/op

BenchmarkGetBytesLockFree8-10           93126028                37.07 ns/op            0 B/op          0 allocs/op
BenchmarkGetBytesLockFree512-10         125311168               33.47 ns/op            0 B/op          0 allocs/op
BenchmarkGetBytesLockFree1024-10        124805904               28.16 ns/op            0 B/op          0 allocs/op
BenchmarkGetBytesLockFree4096-10        110532616               32.00 ns/op            0 B/op          0 allocs/op

BenchmarkGet8-10                        57106448                65.85 ns/op           16 B/op          1 allocs/op
BenchmarkGet512-10                      32213743               113.1 ns/op            16 B/op          1 allocs/op
BenchmarkGetLockFree8-10                61985605                61.30 ns/op           16 B/op          1 allocs/op
BenchmarkGetLockFree512-10              32160564               107.0 ns/op            16 B/op          1 allocs/op

ok      github.com/mbrostami/consistenthash/v2  105.845s


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

## Technical Details

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
