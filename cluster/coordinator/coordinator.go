package coordinator

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/goyal-aman/distributed-storage-nodes/err"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

const (
	Total_Slots = uint64((1 << 64) - 1)
)

const (
	initNodeEndPoint     = "/v1/node/init"
	updateGossipEndpoint = "/v1/gossip"
)

var (
	ErrNodeAlrExistWithEndOfKeyRange = errors.New("node already exist for endOfKeyRange")
	ErrNodeNotFound                  = errors.New("node not found")
)

type Cluster struct {
	// nodes: represent all the storage nodes that are in the
	// system in asc order of node.endOfKeyRange
	nodes []types.StorageNode

	totalSlots uint64
}

// NewCluster creates a cluster coordinator
func NewCluster(host string) *Cluster {

	return &Cluster{
		nodes:      []types.StorageNode{},
		totalSlots: Total_Slots,
	}
}

func (c *Cluster) Info() []types.StorageNode {
	return c.nodes
}

// AddNode
// adds newNode to system.
// when newNode is added to system, before it can start serving traffic it needs to
// get the data it is responsible for from other nodes.
// while the data is being replicated, it doesn't serve traffic.
// while the data is being replicated, any real time writes to data being replicated is also handled
// once all the backlog of data in replicated successfully, it takes over and starts to serve the traffic.
func (c *Cluster) AddNode(node types.StorageNode) error {
	/*
		addNode adds ndoe to coordinator.nodes
	*/

	endOfKeyRange := node.EndOfKeyRange

	originalArr := c.nodes
	// this is the first node
	if len(c.nodes) == 0 {
		if node.EndOfKeyRange != Total_Slots {
			return fmt.Errorf("the first node in the cluster must be has EndOfKeyRange with max value")
		}
		c.nodes = append(c.nodes, node)
	} else {
		index, err := FindInsertIndex(c.nodes, endOfKeyRange)
		if err != nil {
			slog.Error("err in finding nextnode", "err", err, "index", index)
			return fmt.Errorf("err in addNode", err)
		}

		// now that we have index, lets start replication process
		oldNode := c.nodes[index]
		slog.Info("next node found", "endOfKeyRange", endOfKeyRange, "nextNode", oldNode)

		c.nodes = append(c.nodes, types.StorageNode{})
		copy(c.nodes[index+1:], c.nodes[index:])
		c.nodes[index] = node

		// start replication
		replication(node, oldNode)
	}
	slog.Info("add node", "original_arr", originalArr, "new_arr", c.nodes)

	return nil

}

func (c *Cluster) InitNode(node types.StorageNode) error {

	bytesReader := helper.ToBytesReader(map[string]interface{}{
		"Id":            node.Id,
		"Host":          node.Host,
		"EndOfKeyRange": node.EndOfKeyRange,
	})
	resp, err := http.Post(node.Host+initNodeEndPoint, "application/json", bytesReader)
	if err != nil {
		slog.Error("err when init node", "Id", node.Id, "Host", node.Host, "EndOfKeyRange", node.EndOfKeyRange)
		return fmt.Errorf("err occured while send init node", err)
	}

	respBytes := make([]byte, 0)
	resp.Body.Read(respBytes)
	slog.Info("init node success", "Id", node.Id, "Host", node.Host, "EndOfKeyRange", node.EndOfKeyRange, "resp_body", string(respBytes), "resp_status", resp.StatusCode, err)
	return nil
}

// BroadCastNode
// informs all known nodes that new node is now available
func (c *Cluster) BroadCastNode(newNode types.StorageNode) {
	allNodes := c.nodes
	for _, node := range allNodes {
		sendGossip(newNode, node)
	}
}

func sendGossip(newNode, oldNode types.StorageNode) {

	payload := []types.Gossip{
		{
			Id:            newNode.Id,
			Host:          newNode.Host,
			EndOfKeyRange: newNode.EndOfKeyRange,
			LastUpdate:    time.Now(),
		},
	}

	resp, err := http.Post(oldNode.Host+updateGossipEndpoint, "application/json", helper.ToBytesReader(payload))
	if err != nil {
		slog.Error("err when sendGossip", "oldHost", oldNode.Host, "newHost", newNode.Host, err)
		// return fmt.Errorf("err occured while send init node", err)
		return
	}

	respBytes := make([]byte, 0)
	resp.Body.Read(respBytes)
	slog.Info("send node success")
}

func replication(dNode, sNode types.StorageNode) {
	// host := sNode.Host
	// payload := map[string]interface{}{
	// 	"destination":     dNode.Host,
	// 	"destinationEOKR": dNode.EndOfKeyRange,
	// }
	// jsonData, _ := json.Marshal(payload)
	// resp, err := http.Post(host+"/replication", "application/json", bytes.NewBuffer(jsonData))
	// if err != nil {
	// 	panic(err)
	// }

	// respBodyBytes := make([]byte, 0)
	// resp.Body.Read(respBodyBytes)
	// defer resp.Body.Close()

	// slog.Info("replication resp", "resp", string(respBodyBytes))
}

func (c *Cluster) GetNode(key uint64) (*types.StorageNode, error) {
	return helper.GetNode(c.nodes, key)
}


// FindInsertIndex find the index in the array where token should be inserted such that
// array remains sorted in increasing order on node.endOfKeyRange
// which means left most node such that key < node.endOfKeyRange
func FindInsertIndex(nodes []types.StorageNode, token uint64) (int, error) {
	if len(nodes) == 0 {
		return 0, nil
	}
	for i, node := range nodes {
		if token == node.EndOfKeyRange {
			return 0, ErrNodeAlrExistWithEndOfKeyRange
		}
		if token < node.EndOfKeyRange {
			return i, nil
		}
	}
	// given that node.endOfKeyRange talks about the last key in keyrange the node handles.
	// there must be atleast one node in array which must satisfy condition key < node.endOfKeyRange
	return 0, err.ErrNoNodeWithRequiredEndOfKeyRange
}

func (c *Cluster) RemoveNode(node types.StorageNode) error {
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
