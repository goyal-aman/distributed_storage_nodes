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


# Adding Gossip & Enabling nodes to handle all traffic
In my attempt to create distributed storage nodes in order to learn a lot of principles of distributed systems and cassandra. I have gotten this far - I've storage nodes and a coordinator nodes.

coordinator node is also called cluster. 

each time a new node is started, it has no "id", "host" and "EndOfKeyRange" details in it. In cluster there is a endpoint /addnode which add nodes information it in list of known nodes and also updates the node metadata in the node by called /node/init endpoint on storage node. which adds id, host, and EndOfKeyRange details in the node.

EndOfKeyRange property of the storage node directly tells node what range of hashes (hases of key) in the consistent-hash-ring it is responsible for. If a node has EndOfKeyRange as X, then it handles the traffic of hash<=X. FOr more details if there are three nodes in consistent-hash-ring with EndOfKeyRange values of nodea->10, nodeb->50, nodec->100, then nodea handles traffic of all hashes of key <=10, nodeb handles (10,  50], nodec handles (50, 100].

each storage node in the cluster also has gossip behavior, when nodes are added to cluster using /addnode endpoint in cluster, all existing nodes are informed about new-nodes using /updategossip endpoint. when node receives gossip on /updategossip endpoint, they add the new node their their 'gossipNodes' list that they maintain. in the list they maintain information about all known nodes along with the host (ip:port), their id  (uuid), their EndOfKeyRange, and their lastUpdate (timestamp field).

anytime a node recieves get or post traffic, it checks whether it owns the key, that is hash(key) ->token is <= EndOfKeyRange among all nodes, if so then it handles the traffic other wise it find the owner and redirects the traffic to owner nodes and serves the response.

all nodes also gossip on fixed interval letting all known nodes with its own updated timestamp. there fore each node maintains withit all known nodes and their lastKnown update timestamp in cluster (gossip protocol)


# Removing Cluslter Abstraction
1. At present, new node is needed to be added into the cluster manually. When new node is added into cluster using /addnode endpoint, cluster informs all other nodes in the cluster about new node using /upategossip endpoint. Then the nodes take over the responsibility to informing other nodes about their health status using gossip protocol. 
2. Now that I've had more time to think about design and implementation; need of cluster seems unnecessary. I am thinking when new node start up, it can be supplied seed node, that is, the details of existing nodes. New node then can communicate with supplied nodes to get more detailed picture of the cluster.
3. Also, at present when new node is created, the node gets to decide its EndOfKeyRange property. After reading more about it, this is not the normal approach, normally, the node joins the largest range. I am thinking, we can make things easier and support both my approach and the "normal" way. 
4. I am going to implement following functionality. When the first node starts, it alone. There are no seed nodes. It services traffic alone and becomes the seed node for future nodes. Further ndoes that start up, gets the first node details as seed node and with gossip gets more detailed picture into the cluster.
5. When new node starts up, it may optionally provide its EndOfKeyRange. If EndOfKeyRange is provided, it joins the cluster with that range, otherwise, when node starts gossiping - it waits sometime to have more clear picture of the cluster and then - decides its EndOfKeyRange. (Possible: I am aware that it maybe possible that if two nodes start at the same time may get the same picture of the cluster and may decide on same EndOfKeyRange. I will get back to this in future - maybe)
6. Also, when new node starts up and has EndOfKeyRange, it will start to serve data without replication. Replication is  going to nice brain exercise soon.
