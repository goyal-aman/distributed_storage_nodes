package coordinator

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
)

const (
	Total_Slots = uint64((1 << 64) - 1)
)

var (
	ErrNodeAlrExistWithEndOfKeyRange   = errors.New("node already exist for endOfKeyRange")
	ErrNoNodeWithRequiredEndOfKeyRange = errors.New("no node in storage nodes has key < node.endOfKeyRange")
	ErrNodeNotFound                    = errors.New("node not found")
)

type StorageNode struct {
	// EndOfKeyRange is the first last key in the keyrange node is responsible for
	EndOfKeyRange uint64

	// Host is ip:port
	Host string
}

func (s *StorageNode) String() string {
	return fmt.Sprintf("{endOfKeyRange:%d, status:%s}", s.EndOfKeyRange, "unknown")
}

type Cluster struct {
	// nodes: represent all the storage nodes that are in the
	// system in asc order of node.endOfKeyRange
	nodes []StorageNode

	totalSlots uint64
}

// NewCluster creates a cluster coordinator
// new cluster comes with one node out of the node which covers all the keyranges
func NewCluster(host string) *Cluster {

	// this is the only node that is present in the cluster.
	// this node covers all key ranges.
	node := StorageNode{EndOfKeyRange: Total_Slots, Host: host}
	return &Cluster{
		nodes:      []StorageNode{node},
		totalSlots: Total_Slots,
	}
}

func (c *Cluster) Info() []StorageNode {
	return c.nodes
}

func HashKey(key string) uint64 {
	// this returns 256 bit number
	// for our usecase we only care about 64 bits
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8]) % Total_Slots
}

// AddNode
// adds newNode to system.
// when newNode is added to system, before it can start serving traffic it needs to
// get the data it is responsible for from other nodes.
// while the data is being replicated, it doesn't serve traffic.
// while the data is being replicated, any real time writes to data being replicated is also handled
// once all the backlog of data in replicated successfully, it takes over and starts to serve the traffic.
func (c *Cluster) AddNode(node StorageNode) error {
	/*
		addNode adds ndoe to coordinator.nodes
	*/

	endOfKeyRange := node.EndOfKeyRange

	index, err := NextNode(c.nodes, endOfKeyRange)
	if err != nil {
		return fmt.Errorf("err in addNode", err)
	}

	// now that we have index, lets start replication process
	oldNode := c.nodes[index]
	slog.Info("next node found", "endOfKeyRange", endOfKeyRange, "nextNode", oldNode)

	c.nodes = append(c.nodes, StorageNode{})
	copy(c.nodes[index+1:], c.nodes[index:])
	c.nodes[index] = node

	// start replication

	replication(node, oldNode)

	return nil
}

func replication(dNode, sNode StorageNode) {
	host := sNode.Host
	payload := map[string]interface{}{
		"destination":     dNode.Host,
		"destinationEOKR": dNode.EndOfKeyRange,
	}
	jsonData, _ := json.Marshal(payload)
	resp, err := http.Post(host+"/replication", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		panic(err)
	}

	respBodyBytes := make([]byte, 0)
	resp.Body.Read(respBodyBytes)
	defer resp.Body.Close()

	slog.Info("replication resp", "resp", string(respBodyBytes))
}

func (c *Cluster) GetNode(key uint64) (*StorageNode, error) {
	for _, node := range c.nodes {
		if key < node.EndOfKeyRange {
			return &node, nil
		}
	}
	return nil, ErrNoNodeWithRequiredEndOfKeyRange
}

// NextNode find the index in the array where 'key' should be inserted such that
// array remains sorted in increasing order on node.endOfKeyRange
// which means left most node such that key < node.endOfKeyRange
func NextNode(nodes []StorageNode, val uint64) (int, error) {
	if len(nodes) == 0 {
		return 0, nil
	}
	for i, node := range nodes {
		if val == node.EndOfKeyRange {
			return 0, ErrNodeAlrExistWithEndOfKeyRange
		}
		if val < node.EndOfKeyRange {
			return i, nil
		}
	}
	// given that node.endOfKeyRange talks about the last key in keyrange the node handles.
	// there must be atleast one node in array which must satisfy condition key < node.endOfKeyRange
	return 0, ErrNoNodeWithRequiredEndOfKeyRange
}

func (c *Cluster) RemoveNode(node StorageNode) error {
	var index = -1
	for i, cnode := range c.nodes {
		if cnode.EndOfKeyRange == node.EndOfKeyRange && cnode.Host == node.Host {
			index = i
			break
		}
	}
	if index == -1 {
		slog.Info("node not found", "node", node)
		return ErrNodeNotFound
	}

	endOfKeyRange := node.EndOfKeyRange
	if index == len(c.nodes)-1 {
		c.nodes = c.nodes[:index]
		c.nodes[index-1].EndOfKeyRange = endOfKeyRange
		return nil
	}
	// if node at last index, then second last node.EndOfKeyRange is extended removedNode.EndOfKeyRange

	originalArr := c.nodes
	c.nodes = append(c.nodes[:index], c.nodes[index+1:]...)
	slog.Info("node removed", "original", originalArr, "new", c.nodes)
	return nil
}
