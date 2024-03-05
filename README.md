[![Build Status](https://travis-ci.com/mbrostami/consistenthash.svg?branch=master)](https://travis-ci.com/mbrostami/consistenthash)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing

A Go library that implements Consistent Hashing
This package is implemented based on [golang/groupcache](https://github.com/golang/groupcache) package with some improvements

Definitions in this README:
`node`: Refers to the key which is going to be stored in the hash ring or hash table
`rKey`: Refers to the request hash which is not going to be stored, it's used to find the upper closest node in the hash ring to the rKey

### Improvements:

- `Remove` function added - sort and remove a node from the hash ring
- int hashes replaced with uint32
- Number of replicas is now configurable while adding new node (This is useful when capacity is not the same for all nodes)

# Usage

`go get github.com/mbrostami/consistenthash/v2`

### Simple use case
```go
import (
    "fmt"
	
    "github.com/mbrostami/consistenthash/v2"
)

ch := consistenthash.New(WithDefaultReplicas(10))
ch.Add("templateA", "templateB")

assignedTemplate := ch.Get("userA") // assigned template should always be the same for `userA`
fmt.Printf("assigned: %s", assignedTemplate)
```

### Weighted load


```go
import (
    "fmt"
    
    "github.com/mbrostami/consistenthash/v2"
)

ch := consistenthash.New(WithDefaultReplicas(10))
ch.Add("127.0.0.1:1001", "127.0.0.1:1002") 
ch.AddReplicas(40, "127.0.0.1:1003") // 4x more weight 

rKey := "something like request url or user id"
node := ch.Get(rKey) // find upper closest node
fmt.Println(node) // this will print out one of the nodes
```
