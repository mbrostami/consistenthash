[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing

A Go library that implements Consistent Hashing with zero dependencies.
This package is based on [golang/groupcache](https://github.com/golang/groupcache) with a few additions.

### Additions over groupcache:

- `Remove` function added - removes a node (and all of its replicas) from the ring
- int hashes replaced with `uint32`
- Number of replicas is configurable per node via `AddReplicas` (useful when nodes have different capacity)
- Concurrency-safe: all operations are guarded by a single `sync.RWMutex`
- Stored keys are copied, so mutating the caller's buffer afterwards can't corrupt the ring

# Technical Details

The ring is a single slice of positions kept sorted by hash value, plus two maps:

- **ring** `[]node` - every position on the ring (one per replica), sorted by hash. Each entry points back to the original key.
- **hashMap** `map[uint32][]byte` - maps the hash of a key to the original key value.
- **replicaMap** `map[uint32]uint` - records the replica count for a key only when it differs from the default (so adding 10k keys at the default replica count uses zero extra entries here).

### Lookup

`Get` hashes the lookup key and binary-searches the sorted ring for the first
position `>= hash`, wrapping back to the first position when the key falls past
the end of the ring. Time complexity is **O(log N)** where N is the total number
of positions (keys x replicas).

### Adding

`Add` / `AddReplicas` generate the replica positions for each key, append them to
the ring, then sort and de-duplicate. Re-adding the same key is idempotent. The
sort makes the result independent of insertion order, so two rings built from the
same keys always agree.

### Removing

`Remove` recomputes the exact positions a key occupies and filters them out of the
ring in a single pass. It returns `false` if the key was not present.

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
