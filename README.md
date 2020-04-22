# Consistent Hash
A Go library that implements Consistent Hashing

Definitions in this README:  
`node`: Refers to key which is going to be stored in hash ring or hash table    
`rkey`: Refers to the request hash which is not going to be stored, it used to find the upper closest node in hash ring to the rkey  
   
This package is implemented based on  github.com/golang/groupcache/consistenthash package but with some changes:  
- `Remove` function added - O(n) to sort and remove node from hash ring   
- int hashes replaced with uint32  
- Number of replicas is now configurable while adding new node   
**Note**: This is useful when capacity is not the same in all nodes  

# Usage

```
import (
    chash "github.com/mbrostami/consistenthash"
)

// Create CH with 2 replicas
ch := chash.NewConsistentHash(2, nil)
ch.Add("127.0.0.1:1000") // node 1
ch.Add("127.0.0.1:1001") // node 2
ch.Add("127.0.0.1:1002") // node 3

node := ch.Get("something to find the a server for that") // find upper closest node
fmt.println(node) // this will print out one of the nodes  
```

# Test

Benchmark  
`go test -bench=.`   

Tests  
`go test`    
