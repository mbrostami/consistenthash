[![Build Status](https://travis-ci.com/mbrostami/consistenthash.svg?branch=master)](https://travis-ci.com/mbrostami/consistenthash)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing

A Go library that implements Consistent Hashing with zero dependency   
This package is implemented based on [golang/groupcache](https://github.com/golang/groupcache) with some performance improvements

### Improvements:

- `Remove` function added - sort and remove a node (and replicas) from the hash ring
- int hashes replaced with uint32
- Number of replicas is now configurable while adding new node (This is useful when capacity is not the same for all nodes)

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
