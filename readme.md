# Distributed Storage Nodes

[![Go Version](https://img.shields.io/badge/Go-1.25+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A hands-on implementation of distributed storage principles inspired by Apache Cassandra. This project demonstrates core distributed systems concepts including consistent hashing, gossip protocols, data replication, and fault tolerance through a cluster of storage nodes.

## 🚀 What This Project Is

I've been building this distributed storage system to better understand how databases like Cassandra work in production. It's not meant to be a production-ready system, but rather a learning tool to explore concepts like:

- How consistent hashing distributes data across nodes
- How gossip protocols handle node discovery and failure detection  
- How data replication works when adding new nodes to a live system
- How node state management works during the join/bootstrap process
and more

This is very much a work-in-progress and learning project. I'm implementing these concepts step by step, making mistakes along the way and documenting my thought process in the design files.

## ✨ Key Features

- **Decentralized Architecture**: No single point of failure—nodes communicate peer-to-peer
- **Dynamic Node Addition**: Seamlessly add new storage nodes with automatic data replication
- **Gossip-Based Discovery**: Automatic node discovery and health monitoring
- **Consistent Hashing**: Efficient data distribution using hash rings
- **Live Replication**: Real-time data synchronization during node bootstrapping
- **REST API**: Clean HTTP interfaces for all operations
- **In-Memory Storage**: Fast key-value operations 

## 🚧 Development Roadmap

### ✅ Core Features (Implemented)
- Decentralized peer-to-peer node communication
- Consistent hashing with configurable key ranges
- Gossip protocol for cluster membership
- Live data replication during node bootstrapping
- RESTful APIs for data operations
- Node state management (JOINING → BOOTSTRAPPING → AVAILABLE)

### 🔄 Up Next (Active Development)
- **Non-blocking Replication**: At present, point-in-time snapshot of datastore stops all writes. Goal is to learn, develop & implement point-in-time snapshot. So Far, I am aware of LSM Tree based approach. More details to be researched.

### 📋 Backlog (Future Enhancements)
- **Persistent Storage Backend**: Replace in-memory storage with disk persistence
- **Node Failure Detection**: Enhanced gossip with heartbeat monitoring
- **Data Replication for Fault Tolerance**: Multi-replica support with configurable replication factor
- **Quorum & Consistency Levels**: Tunable consistency (strong/eventual) with read/write quorums
- **Node Removal**: Graceful decommissioning of storage nodes
- **Compaction**: Data cleanup and storage optimization

*Got ideas for the roadmap? Open an issue or PR!*

## 🏗️ Architecture Overview

### Node States
- **JOINING**: New node gathering cluster topology
- **BOOTSTRAPPING**: Replicating data from existing nodes
- **AVAILABLE**: Actively serving read/write requests

### Data Flow
1. **Consistent Hashing**: Keys are hashed and assigned to nodes based on hash ranges
2. **Gossip Protocol**: Nodes exchange membership and health information
3. **Replication**: New nodes replicate data from predecessors during bootstrapping
4. **Traffic Routing**: Any node can handle requests and route to the correct owner

## 🛠️ Quick Start

### Prerequisites
- Go 1.25+
- Basic understanding of distributed systems (optional but helpful)

### Installation

```bash
git clone https://github.com/goyal-aman/distributed-storage-nodes.git
cd distributed-storage-nodes
go mod download
```

### Running Your First Cluster

Start the seed node (handles the full hash range):

```bash
make seed
# or manually:
go run nodes/main.go -eokr=18446744073709551615 -host=http://0.0.0.0:7770
```

Add a second node (handles half the range):

```bash
make node1
# or manually:
go run nodes/main.go -port=7771 -seed=http://0.0.0.0:7770 -eokr=9223372036854775808 -host=http://0.0.0.0:7771
```

Add a third node:

```bash
make node2
# or manually:
go run nodes/main.go -eokr=4611686018427387904 -host=http://0.0.0.0:7772 -port=7772 -seed=http://0.0.0.0:7770
```

## 📡 API Usage

### Store Data
```bash
curl -X POST http://localhost:7770/v1/data \
  -H "Content-Type: application/json" \
  -d '{"key": "mykey", "value": "myvalue"}'
```

### Retrieve Data
```bash
curl http://localhost:7770/v1/data/mykey
```

### Check Node Status
```bash
curl http://localhost:7770/v1/node/detail
```

### View Gossip Information
```bash
curl http://localhost:7770/v1/gossip
```

## 🔍 Understanding the Implementation

### Consistent Hashing
Each node owns a portion of the 64-bit hash space. The `EndOfKeyRange` parameter defines the upper bound of keys a node handles.

### Gossip Protocol
Nodes periodically exchange information about cluster membership, ensuring all nodes have a consistent view of the topology.

### Replication Strategy
When a new node joins:
1. It enters JOINING state and gossips to learn the cluster
2. Transitions to BOOTSTRAPPING and requests data replication
3. Receives point-in-time snapshots + live mutations
4. Becomes AVAILABLE and starts serving traffic

## 🎯 What I've Learned

Through building this system, I've gained hands-on experience with:

- Implementing consistent hashing algorithms from scratch
- Building gossip protocols for distributed node discovery
- Handling data replication in live systems with ongoing writes
- Managing node state transitions during cluster changes
- Understanding the core principles behind Cassandra's architecture

## 📚 Documentation

- [Design Thoughts](learnings/design_thoughts.md) - My detailed thought process and implementation decisions
- [Consistent Hashing](learnings/consistent_hashing.md) - Deep dive into the hashing algorithm and my learnings
- [API Specifications](api/) - Complete API documentation

## 🤝 Contributing

I'm open to contributions and feedback! If you're learning distributed systems too, feel free to:

- Try out the code and share your findings
- Suggest improvements or point out issues
- Add features you're interested in exploring
- Ask questions about the implementation

## 📄 License

MIT License - feel free to use this for your own learning and experimentation.

## 🙏 Acknowledgments

This project is my attempt to understand the fascinating world of distributed systems. Huge thanks to the Cassandra community and the broader distributed systems field for the inspiration and knowledge shared openly.

---

**Want to learn distributed systems too?** Start with `make seed` and see how the nodes discover and replicate with each other!</content>
<parameter name="filePath">/Users/amangoyal/code/distributed_storage_nodes/readme.md
