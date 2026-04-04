# Distributed Storage Nodes
Distributed storage nodes is developed to gain the working knowledge of how storage system like cassandra databases work in production and provide some of the features. With this project I am particullarly interested in learning
1. how cassandra does replication using merkle tree on live production data which has writes happening
2. writes to storage nodes are handled to provide durability
3. reads are offered by storage nodes
4. quorom is implemented to prodivde tunable consistency
5. understand node failure are handled. Particularly, I want to understand how other nodes decide a particular node is dead and then take over.
6. how consistent hashing is implemented. With this I am particularly interested in understanging how nodes take over the load of dead nodes
7. how is it decided that which node will handle the incoming request.
8. Understand the working of gossip protocol.
9. Implement a method such that any node can handle arbitary traffic, such that, if it receives traffic for request/data which it doesn't own then it redirects the traffic to original node.

## PRD
Go is used, because I am familiar with it and nice developer experience.

1. There are storage nodes. 
2. Each storage node handles subset of data based on key range.
3. Cluster is collection of storage node, with atleast 1 storage node.
4. Communication happens using REST over HTTP. For the purpose of this project REST is sufficient.
5. We can remove the concept of cluster or coordinator node entirely, and when an node starts we can supply the seed node which can act as cluster node.