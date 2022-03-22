[![Build Status](https://travis-ci.com/mbrostami/consistenthash.svg?branch=master)](https://travis-ci.com/mbrostami/consistenthash)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrostami/consistenthash)](https://goreportcard.com/report/github.com/mbrostami/consistenthash)

# Consistent Hashing
A Go library that implements Consistent Hashing  
This package is implemented based on  [golang/groupcache](https://github.com/golang/groupcache) package with some improvements

Definitions in this README:  
`node`: Refers to the key which is going to be stored in the hash ring or hash table    
`rKey`: Refers to the request hash which is not going to be stored, it's used to find the upper closest node in the hash ring to the rKey  
   

### Improvements:  
- `Remove` function added - sort and remove a node from the hash ring   
- int hashes replaced with uint32  
- Number of replicas is now configurable while adding new node   
**Note**: This is useful when capacity is not the same for all nodes  

# Usage
`go get github.com/mbrostami/consistenthash`  


```
import (
    "github.com/mbrostami/consistenthash"
)

// Create ConsistentHash with 2 replicas
ch := consistenthash.New(2, nil)
ch.Add("127.0.0.1:1001") // node 1
ch.Add("127.0.0.1:1002") // node 2
ch.AddReplicas("127.0.0.1:1003", 4) // node3 has more capacity so possibility to get assigned request is higher than other nodes 

rKey := "something like request url"
node := ch.Get(rKey) // find upper closest node
fmt.println(node) // this will print out one of the nodes  
```

# Test

Benchmark  
`go test -bench=.`   

Race Condition 
`go test --race`  
Thanks to **Mohammad Rajabloo** for reporting the race issue 

Tests  
`go test`    
