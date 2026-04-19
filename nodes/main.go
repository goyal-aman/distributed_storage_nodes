package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/goyal-aman/distributed-storage-nodes/cluster/coordinator"
	pkgerr "github.com/goyal-aman/distributed-storage-nodes/err"
	"github.com/goyal-aman/distributed-storage-nodes/gossip"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	apiclient "github.com/goyal-aman/distributed-storage-nodes/nodes/api_client"
	"github.com/goyal-aman/distributed-storage-nodes/store"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

const (
	TOTAL_SLOTS               = uint64((1 << 64) - 1)
	BROADCAST_TICKER_DURATION = 3 * time.Second

	// WRITE_CONFIRMATION_TIMEOUT
	// this is the max duration for which nodes are awaited for success of
	// replicating write to peer nodes
	WRITE_CONFIRMATION_TIMEOUT = 1 * time.Second
)

var (
	storagev2 = store.NewDataStore()
)

var (
	fPort          = flag.Int("port", 7770, "port of application, default is 7770")
	fHost          = flag.String("host", "", "host addr of current node. this is mandatory. example 'http://0.0.0.0:7770'")
	fEndOfKeyRange = flag.String("eokr", "", "EndOfKeyRange for the node. this is mandatory. '18446744073709551615' is max value")
	fSeedNodes     = flag.String("seed", "", "comma separated host details of seed node, default is ''; example 'http://0.0.0.0:8080,http://0.0.0.0:8081'")

	fReplicaCount = flag.String(
		"replicacount", "",
		"additional nodes in cluster which contains the copy of data. If replicacount is 2, that means there are total three copies - 1 ownernode and 2 replicanodes"+
			"replicaCount can be supplied to seed nodes. That is, if a node is started with seed arg then replicacount cannot be provided")
)

var (
	GVar_PORT int

	// GVar_Host is host address of current node where other nodes will contact on
	GVar_Host          string
	GVar_SeedNodes     []string
	GVar_ReplicaCount  int
	GVar_EndOfKeyRange uint64
)

var (
	updateGossipEndpoint = "/v1/gossip"
)

func init() {
	flag.Parse()

	// handle Port
	GVar_PORT = *fPort

	// handle host
	if fHost == nil || len(*fHost) == 0 {
		slog.Error("host is mandatory")
		flag.Usage()
		os.Exit(1)
	} else {
		// TODO: add check to ensure host format is correct
		GVar_Host = *fHost
	}

	// handle seedNode
	if fSeedNodes != nil && len(*fSeedNodes) > 0 {
		GVar_SeedNodes = strings.Split(*fSeedNodes, ",")
		slog.Info("Seed Nodes", "seed_nodes", GVar_SeedNodes)
	}

	if len(GVar_SeedNodes) > 0 && len(strings.Trim(*fReplicaCount, " ")) > 0 {
		// if this is not seed node then replica count cannot be provided
		slog.Error("replicacount can only be supplied to seednode")
		os.Exit(1)
	} else if len(GVar_SeedNodes) == 0 && len(strings.Trim(*fReplicaCount, " ")) == 0 {
		// if this is seed node that replica count is must
		slog.Error("for seed node 'replicacount' must be provided")
		os.Exit(1)
	} else if len(GVar_SeedNodes) == 0 {
		slog.With("received replicacount", *fReplicaCount).
			Info("initializing replicaclount")
		iVal, err := strconv.Atoi(*fReplicaCount)
		if err != nil {
			slog.With("err", err).Error("invalid replicacount")
			os.Exit(1)
		}
		slog.With("received replicacount", *fReplicaCount).
			With("parsed replicacount", iVal).
			Info("initialised replicaclount")
		GVar_ReplicaCount = iVal

	}

	// handle EndOfKeyRange
	if fEndOfKeyRange == nil || len(*fEndOfKeyRange) == 0 {
		slog.Error("eokr (EndOfKeyRange is mandatory)")
		flag.Usage()
		os.Exit(1)
	} else {
		endOfKeyRange, err := helper.StrToUInt64(*fEndOfKeyRange)
		if err != nil {
			slog.Error("invalid value of eokr (EndOfKeyRange)", "err", err)
			flag.Usage()
			os.Exit(1)
		}
		GVar_EndOfKeyRange = endOfKeyRange
		slog.Info("EndOfKeyRange", "eokr", GVar_EndOfKeyRange)
	}
}

type Node struct {
	Id            string
	Host          string
	EndOfKeyRange uint64

	GossipV2 *gossip.Gossip[string, types.NodeGossip]
	// Gossip   map[string]types.NodeGossip

	LastUpdate time.Time
	State      types.NodeState

	ReplicaCount int

	// isReadyForBootstrapping
	gossipMeta []int
}

func (n *Node) XEndOfKeyRange() uint64 {
	return n.EndOfKeyRange
}

// NewNode
// if seed nodes are provided, then reaches out to seed nodes and get their details
// if successfully receives the details from seed node, then put them as their gossip partner
// if seed nodes are provided but due to some error they are not available
// nodes are without seeds (and gossip)
func NewNode() *Node {
	gossipNodesV2 := gossip.NewGossip[string, types.NodeGossip]()
	if len(GVar_SeedNodes) > 0 {
		for _, sNode := range GVar_SeedNodes {
			nodeMeta, err := apiclient.GetNodeMeta(sNode)
			// slog.Info("seed node data", "node_meta", nodeMeta)
			if err != nil {
				slog.Error("failed to get seed nodemeta", "err", err, "host", sNode)
				continue
			}

			endOfKeyRange := uint64(nodeMeta["EndOfKeyRange"].(float64))

			// str := "2026-04-04T16:22:05.828432+05:30"
			t, terr := time.Parse(time.RFC3339Nano, nodeMeta["LastUpdate"].(string))
			if terr != nil {
				slog.Error("error in getting EndOfKeyRange of seednode", "err", err, "host", sNode)
				continue
			}

			id := nodeMeta["Id"].(string)
			host := nodeMeta["Host"].(string)

			replicaCount := int(nodeMeta["ReplicaCount"].(float64))
			state := types.NodeState(nodeMeta["State"].(string))
			a := types.NodeGossip{
				Id:            id,
				Host:          host,
				EndOfKeyRange: endOfKeyRange,
				LastUpdate:    t,
				State:         state,
				ReplicaCount:  replicaCount,
			}
			gossipNodesV2.Upsert(id, a)

			// update GVar_ReplicaCount
			slog.With("seed_replica_count", replicaCount).
				With("current_replica_count", GVar_ReplicaCount).
				With("num_seed_nodes", len(GVar_SeedNodes)).
				Info("initing GVar_ReplicaCount")
			if GVar_ReplicaCount == 0 {
				GVar_ReplicaCount = replicaCount
			} else if GVar_ReplicaCount > 0 && GVar_ReplicaCount != replicaCount {
				slog.Error("Conflicting replicacount")
				os.Exit(1)
			}
			// gossipNodes[id] = a
		}
		slog.Info("seed node found", "seed_node", gossipNodesV2)
	}

	// all node starts with "JOINING" state in existing
	//  cluster. For new clusters, nodes are immediately available
	nodeState := types.JOINING
	if gossipNodesV2.Size() == 0 {
		nodeState = types.AVAILABLE
	}

	slog.With("GVar_replicacount", GVar_ReplicaCount).Info("creating node")
	node := &Node{
		Id:            uuid.New().String(),
		Host:          GVar_Host,
		GossipV2:      gossipNodesV2,
		EndOfKeyRange: GVar_EndOfKeyRange,
		LastUpdate:    time.Now(),
		State:         nodeState,
		ReplicaCount:  GVar_ReplicaCount,
	}

	node.GossipV2.Upsert(node.Id, types.NodeGossip{
		Id:            node.Id,
		Host:          node.Host,
		EndOfKeyRange: node.EndOfKeyRange,
		LastUpdate:    time.Now(),
		State:         node.State,
		ReplicaCount:  node.ReplicaCount,
	})

	slog.Info("node created", "node", node)
	return node
}

func main() {
	node := NewNode()
	router := initRouter(node)

	go func() {
		ticker := time.NewTicker(BROADCAST_TICKER_DURATION)
		for t := range ticker.C {
			node.BroadcastGossip(t)
		}
	}()

	if err := router.Run(fmt.Sprintf("0.0.0.0:%d", GVar_PORT)); err == nil {
		slog.Info("listening on", "port", GVar_PORT)
	}
}

func initRouter(node *Node) *gin.Engine {
	router := gin.New()

	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: []string{"/v1/gossip"},
	}))

	// 2. Add Recovery middleware (since we didn't use gin.Default)
	router.Use(gin.Recovery())

	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	// V1_NODE node meta
	router.GET(apiclient.V1_METADATA_NODE.String(), node.getMetaDataNode)

	// POST V1_DATA make routing decisions
	// sends data to owner node
	// if owner node receives the data,
	// it also replicates to replicas
	router.POST(apiclient.V1_DATA.String(), node.postData)
	router.GET(apiclient.V1_DATA.String(), node.getData)

	// V1_DATA_REPLICA directly persists the data
	// this endponit doesn't have any routing inteligence
	router.POST(apiclient.V1_REPLICA_DATA.String(), node.postReplicaData)

	// V1_DATA_REPLICATION
	router.GET(apiclient.V1_SNAPSHOT_STREAM.String(), node.getSnapshotStream)

	// V1_GOSSIP
	router.POST(apiclient.V1_GOSSIP.String(), node.postGossip)
	router.GET(apiclient.V1_GOSSIP.String(), node.getGossip)

	// V1_KEYDETAIL return who own the key
	router.GET(apiclient.V1_METDATA_KEY.String(), node.getMetaDataKey)

	// V1_SNAPSHOT
	// exposed this api for debugging purposes
	// not needed by any node/client
	router.GET(apiclient.V1_SNAPSHOT.String(), node.getSnapShot)

	return router

}

// getMetaDataNode
// returns present state of node
func (n *Node) getMetaDataNode(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"Id":            n.Id,
		"Host":          n.Host,
		"Gossip":        n.GossipV2.Read(),
		"EndOfKeyRange": n.EndOfKeyRange,
		"LastUpdate":    n.LastUpdate,
		"State":         n.State,
		"ReplicaCount":  n.ReplicaCount,
	})
}

func (n *Node) changeStateToBootStrappingIfReady() bool {
	if n.State != types.JOINING {
		return false
	}
	slog.Info("checking for bootstrapping")
	numNodes := n.GossipV2.Size()
	n.gossipMeta = append(n.gossipMeta, numNodes)

	if len(n.gossipMeta) >= 3 &&
		n.gossipMeta[len(n.gossipMeta)-1] == n.gossipMeta[len(n.gossipMeta)-2] &&
		n.gossipMeta[len(n.gossipMeta)-2] == n.gossipMeta[len(n.gossipMeta)-3] {
		n.State = types.BOOTSTRAPPING
		go n.BroadcastGossip(time.Now()) //inform all other nodes of state change
		slog.Info("changing state to boostrapping")
		go n.handleBootstrapping()
		return true
	}
	return false
}

func (n *Node) handleBootstrapping() {
	// find source node of data
	// find node just to the right
	gossipNodes := helper.MapToArr(n.GossipV2.Read())

	nextNode, err := helper.GetNodeByToken(gossipNodes, n.EndOfKeyRange+1)
	if err != nil {
		slog.Error("cannot find next node in bootstrapping", "err", err)
		return
	}
	fmt.Println(nextNode)

	// TODO: currently I am not a huge fan of how request snapshot
	// and dual writes are working together.
	// in current implementation, state of curr node is changed to
	// BOOTSTRAPPING, then snapshot is requested.
	// it is possible that when current node changes its state from JOINING
	// to BOOTSTRAPPING, it immediate request for snapshot form the owner node.
	// The ownernode, on getting request of snapshot, immediatly creates point-in-time
	// snapshot and streams it back to new node. In this the ownernode hasn't actually
	// started dual writing new changes to new node, this is because it is possible
	// new node might get update after some delay that new node has changed its state to
	// BOOTSTRAPPING.
	// I am thinking that when new node requests the point-in-time snapshot for streaming
	// original node should use same connection/stream to stream new changes back to
	// new node. When snapshot have been streamed completely, the owner node
	// can send snapshot-completion event back to owner node while continuing
	// to stream change events to new-node. New node on receiving snapshot completion
	// event will change its state to "AVAILABLE" and broadcast new state to other nodes.
	// All nodes, including original node, after receing latest state of new-node as "JOINING"
	//  will redirect all related traffic to new-node instead of original node.
	// which means original node will eventually reach a point where it has no mutations/writes
	// to stream back to. At this point, the replication stream request can be closed successfully.

	slog.Info("requesting replica")

	streamCh, err := apiclient.RequestSnapshot(nextNode.Host, n.Id)
	if err != nil {
		slog.Error("error in requesting replica", "err", err)
		return
	}
	slog.Info("consuming replication stream")
	for ch := range streamCh {
		switch ch.EType {
		case types.SnapshotReplicationEType:
			event := ch.SnapshotReplicationEvent
			for _, v := range event.Values {
				storagev2.PutRaw(event.Key, v.Value, &v.Version, false)
			}
		case types.SnapshotReplicationCompleteEType:
			slog.Info("snapshot replication completed, updating state to available")
			n.State = types.AVAILABLE

		case types.LiveMutationReplicationEType:
			event := ch.LiveMutationReplicationEvent
			storagev2.PutRaw(event.Key, event.Value, &event.Version, false)
		case types.LiveMutationReplicationCompleteEType:
			slog.Info("live mutations completed")
		default:
			slog.Warn("unknown event type in replication stream")
		}
	}

	slog.Info("replication completed,serving traffic")
}

// BroadcastGossip
// sends lastKnownTime of known nodes to all other known nodes
// ideally it should pick and choose, but for the purpse of this
// learning exercise, we are sending to all. It could choose randomly
// which is trivial part.
func (n *Node) BroadcastGossip(_ time.Time) {
	if n.Id == "" {
		return
	}

	// update currNode lastUpdate
	currNode := types.NodeGossip{
		Id:            n.Id,
		Host:          n.Host,
		EndOfKeyRange: n.EndOfKeyRange,
		LastUpdate:    time.Now(),
		State:         n.State,
		ReplicaCount:  n.ReplicaCount,
	}
	n.GossipV2.Upsert(n.Id, currNode)

	gossipMap := n.GossipV2.Read()
	gossipArr := make([]types.NodeGossip, len(gossipMap))
	idx := 0
	for _, val := range gossipMap {
		gossipArr[idx] = val
		idx++
	}

	for id, g := range gossipMap {
		if id == n.Id {
			continue
		}
		slog.Debug("Sending gossips", "from", *fPort, "to", g.Host, "data", gossipArr)
		sendGossip(g.Host, helper.ToBytesReader(gossipArr))
	}
	slog.Debug("Broadcast success")

}

func sendGossip(destHost string, bytesReader *bytes.Reader) {
	resp, err := http.Post(destHost+updateGossipEndpoint, "application/json", bytesReader)
	if err != nil {
		slog.Error("err when sendGossip", "destHost", destHost, "err", err)
		// return fmt.Errorf("err occured while send init node", err)
		return
	}

	respBytes := make([]byte, 0)
	resp.Body.Read(respBytes)
	slog.Debug("send gossip success", "destHost", destHost, "status", resp.StatusCode, "resp_body", string(respBytes))

}

// postGossip
// updates gossip using lists of gossips from clients
func (n *Node) postGossip(c *gin.Context) {
	body := []types.NodeGossip{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
		return
	}

	// beforeUpdateGossip := logIDAndTime2(n.Gossip)
	// receviedUpdates := logIDAndTime(body)

	for _, g := range body {
		val, exist := n.GossipV2.Get(g.Id)
		if exist {
			if g.LastUpdate.After(val.LastUpdate) {
				val.LastUpdate = g.LastUpdate
				val.State = g.State
				n.GossipV2.Upsert(g.Id, *val)
			}
		} else {
			n.GossipV2.Upsert(g.Id, g)
		}
	}

	n.changeStateToBootStrappingIfReady()

	c.JSON(http.StatusOK, gin.H{
		"message": n.GossipV2.Read(),
	})
}

// getGossip
// return the list of gossips
func (n *Node) getGossip(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": n.GossipV2.Read(),
	})
}

// GetDefaultWriteQuorum return the count of replicas
// that should have the key given ecluding owner node.
// Total number of nodes which have keys is replicas + owner-node
func GetDefaultWriteQuorum() string {
	wq := GVar_ReplicaCount / 2
	return fmt.Sprintf("%d", wq)
}

// postReplicaData
func (n *Node) postReplicaData(c *gin.Context) {
	body := types.HandlePostRawReq{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, types.PostRawDataResponse{
			Message: "something wrong with body",
			Err:     err.Error(),
		})
		return
	}
	_, err := storagev2.PutRaw(body.Key, body.Value, &body.Version, true)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.PostRawDataResponse{
			Err:     err.Error(),
			Message: "error while putting raw val in store",
		})
		return
	}
	c.JSON(http.StatusOK, types.PostRawDataResponse{
		IsSuccess: true,
		Message:   "OK",
	})

}

// postData
// finds the owner node of key and store value against the key in that node.
// value can be any type.
//
// Query Params:
// writequorum: valid values in [-inf to ReplicaCount]. writequorum is number
// replicas which must confirm write for write to be confirmed success within
// WRITE_CONFIRMATION_TIMEOUT duration. Negative values means all replicas
// must confirm write. Owner node is always required to confirm the write.
// if for any reason owner node fails the write the request is failed and
// not replicated to replicas
//
// include: comma separated list of strings. This is used to return additional
// metadata in the response. Mainly added this for debugging purpose. Presently
// support values are "res". This return the result from replica nodes in the
// response metadata
func (n *Node) postData(c *gin.Context) {
	body := map[string]interface{}{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, types.PostDataResponse{
			Message: "something wrong with body",
			Err:     err.Error(),
		})
		return
	}

	key := body["key"].(string)
	value := body["value"]
	writequorum := c.Query("writequorum")
	slog.With("supplied_write_quorum", writequorum).Info("")
	if strings.TrimSpace(writequorum) == "" {
		writequorum = GetDefaultWriteQuorum()
		slog.With("updated_write_quorum", writequorum).Info("")
	}

	// WriteQuorum
	// for WriteQuorum < 0, all nodes must return confirmation
	// for WriteQuorum > 0, then provided number of nodes are awaited
	// before write is confirmed up until timout
	writequorumInt := 0
	if v, err := strconv.Atoi(writequorum); err != nil {
		c.JSON(http.StatusBadRequest, types.PostDataResponse{
			Err:     err.Error(),
			Message: "writequorum must be valid int",
		})
		return
	} else if v < 0 {
		writequorumInt = n.GossipV2.Size()
	} else {
		writequorumInt = v
	}

	includeQueryParameter := c.Query("include")
	includeSlice := strings.Split(includeQueryParameter, ",")

	ar := helper.MapToArr(n.GossipV2.Read())
	sort.Slice(ar, func(a, b int) bool {
		// increasing order in EndOfKeyRange
		return ar[a].EndOfKeyRange < ar[b].EndOfKeyRange
	})

	token := helper.HashKey(key, TOTAL_SLOTS)
	ownerNode, err := helper.GetNode(ar, token)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.PostDataResponse{
			Err:     err.Error(),
			Message: "err while getting owner node",
		})
		return
	}

	// slog.With("current_node", n.Id).
	// 	With("owner_node", ownerNode.Id).
	// 	With("key", key).
	// 	With("token", token).
	// 	With("gossips", n.GossipV2.Read()).
	// 	Info("owner node details")

	// is someother node own the key
	// get data from it
	if ownerNode.Id != n.Id {
		queryParams := map[string]string{
			"writequorum": writequorum,
			"redirected":  "true",
		}
		rerr := apiclient.PostKeyValue(*ownerNode, key, value, queryParams)
		if rerr != nil {
			c.JSON(http.StatusInternalServerError, types.PostDataResponse{
				Message: "err posting value to owner node",
				Err:     rerr.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, types.PostDataResponse{
			IsSuccess: true,
			Message:   "OK",
			Metadata: &types.PostDataMetaData{
				Redirected: true,
				ServicedBy: n.Id,
				OwnedBy:    ownerNode.Id,
			},
		})

		return
	}

	// send request to next replicacount number of nodes
	// wait for only writequorum number of nodes
	// writequorum of <=1 is considered as 1
	// if number of nodes in the cluster are less then replicacount
	// then fail the write
	if n.GossipV2.Size() < GVar_ReplicaCount {
		c.JSON(http.StatusFailedDependency, types.PostDataResponse{
			Err:     pkgerr.ErrNotEnoughNodes.Error(),
			Message: "numer of nodes in cluster should be atleast equal to replicacount",
		})
		return
	}

	// current node own the key range
	// store in the databaase, additionally, if there is node
	// with EndOfKeyRange just less than curr node.EndOfKeyRange
	// and State == BootStrapping then this write needs to be replicated there.
	// Later on I'll check whether this can be achieved by raft algo
	newVersion, err := storagev2.Put(key, value)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.PostDataResponse{
			Message: "error in writing value to owner node, write not replicated to replicas",
			Err:     err.Error(),
		})
		return
	}

	// get next replicacount number of nodes
	ownerAndReplicas, err := helper.GetNNode(ar, token, GVar_ReplicaCount)
	if err != nil {
		slog.With("err", err).Error("err in replicating write")
		return
	}

	// owner := ownerAndReplicas[0]
	replicas := ownerAndReplicas[1:]
	f := func(ctx context.Context, n types.NodeGossip) (*types.NodeGossip, error) {
		return &n, apiclient.PostRawKeyValue(ctx, n, key, value, newVersion)
	}
	// slog.With("ownerAndReplicas", ownerAndReplicas).
	// 	With("ar", ar).
	// 	With("gossip", n.GossipV2.Read()).
	// 	Info("calling post replica data")

	res := helper.RunUntilMinSuccessOrTimeout(f, replicas, writequorumInt, WRITE_CONFIRMATION_TIMEOUT)

	count := 0
	for _, r := range res {
		if r.E == nil {
			count++
		}
	}

	mislaneous := map[string]interface{}{}
	if slices.Contains(includeSlice, "res") {
		mislaneous["res"] = res
	}

	c.JSON(http.StatusOK, types.PostDataResponse{
		IsSuccess: true,
		Message:   "OK",
		Metadata: &types.PostDataMetaData{
			Redirected:   true,
			ServicedBy:   n.Id,
			OwnedBy:      ownerNode.Id,
			ReplicaCount: count,
			Mislaneous:   mislaneous,
		},
	})

}

func (n *Node) getData(c *gin.Context) {
	key := c.Query("key")
	slog.With("key", key).Info("get data")

	ar := helper.MapToArr(n.GossipV2.Read())
	helper.SortNodeInPlace(ar)

	token := helper.HashKey(key, TOTAL_SLOTS)
	ownerNode, err := helper.GetNode(ar, token)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	// is someother node own the key
	// get data from it
	if ownerNode.Id != n.Id {
		respBody, rerr := apiclient.GetKeyValue(*ownerNode, key)
		if rerr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": rerr.Error(),
			})
		} else {
			respBody["metadata"] = map[string]string{
				"redirected":  "true",
				"serviced_by": n.Id,
				"owned_by":    ownerNode.Id,
			}

			c.JSON(http.StatusOK, respBody)
		}
		return
	}

	if val, err := storagev2.Get(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": "ok", "value": val})

	}
}

// nodedetail return details of node own the 'key'
func (n *Node) getMetaDataKey(c *gin.Context) {
	key := c.Query("key")
	hash := helper.HashKey(key, coordinator.Total_Slots)

	ar := helper.MapToArr(n.GossipV2.Read())
	sort.Slice(ar, func(a, b int) bool {
		// increasing order in EndOfKeyRange
		return ar[a].EndOfKeyRange < ar[b].EndOfKeyRange
	})

	// find node which handles this hash range
	node, err := helper.GetNode(ar, hash)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"node": node,
	})
}

func (n *Node) getSnapshotStream(c *gin.Context) {
	slog.Info("in start replication")
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

	// for replicating snapshot
	replSSCh := make(chan types.SnapshotReplicationEvent)

	// for replicating writes/mutations that come in
	// during replication
	replLiveMutationChan := make(chan types.LiveMutationReplicationEvent)

	// used to verify state of sourceid
	// if sourceid.state is AVAILABLE and there
	// are no live writes/mutation replicate then
	// it is safe to close chan responsible for
	// replicating live writes/mutation
	sourceNodeId := c.Query("sourceid")
	lastLiveMutationSentTimeStamp := time.Now()

	// call this hook after snapshot is replicated
	// and sourceId node is in "AVAILABLE" state
	var postWriteHookReset store.PostWriteHookCncl

	// isReplSnapshotCompletionEventSent
	// if true it means snapshot replication is complete
	isReplSnapshotCompletionEventSent := false

	// isLiveMutationCompleteEventSent
	// if true, it means live mutations are completed
	isLiveMutationCompleteEventSent := false

	// periodically check if replication is completed
	// once completed, callPostWriteHook
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			sourceNode, exist := n.GossipV2.Get(sourceNodeId)
			if !exist {
				continue
			}
			sourceState := sourceNode.State

			// liveMutation can only be stopped after
			// replication is completed and source node
			// is "AVAILABLE" in state, and last mutation
			// sent was over 10 second ago. 10 second is
			// arbitary choice.
			if isReplSnapshotCompletionEventSent &&
				sourceState == types.AVAILABLE {

				close(replLiveMutationChan)
				postWriteHookReset()
				ticker.Stop()
				slog.Info("Live Mutation Completed")
				break
			} else {
				slog.Info("live mutaion continuing", "isReplSnapshotCompletionEventSent", isReplSnapshotCompletionEventSent,
					"sourceState", sourceState, "lastMutationTimeStampDiff", time.Since(lastLiveMutationSentTimeStamp).Seconds())
			}
		}
	}()

	// replicate snapshot
	go func() {
		defer close(replSSCh)
		numKeys := storagev2.Size()
		if numKeys == 0 {
			postWriteHookReset = func() { slog.Info("calling defualt postwrite hook") }
			return
		}

		sourceNode, exist := n.GossipV2.Get(sourceNodeId)
		if !exist {
			slog.Error("snapshot replication not started, couldn't find source node in gossips", "sourceNodeId", sourceNodeId)
			return
		}
		keyFilter := func(key string) bool {
			token := helper.HashKey(key, TOTAL_SLOTS)
			return token <= sourceNode.EndOfKeyRange
		}

		// set postwrite hook such that new mutations/writes
		// are passed to replLiveMutationch channel
		snapshot, cncl := storagev2.Snapshot(
			keyFilter,
			store.WithPostWriteHook(func(key string, val any, version uint64) {
				token := helper.HashKey(key, TOTAL_SLOTS)
				if token <= sourceNode.EndOfKeyRange {
					body := types.LiveMutationReplicationEvent{
						Etype:   types.LiveMutationReplicationEType,
						Key:     key,
						Value:   val,
						Version: version,
					}
					replLiveMutationChan <- body
				}
			}),
		)
		postWriteHookReset = cncl
		slog.Info("Assigned postWriteHook", "hook", postWriteHookReset)
		slog.Info("complete source snapshot", "snapshot", snapshot)
		for key, values := range snapshot {
			// at present values which is array of all versions
			// is returned, possibly in future, it can be looked
			// upon if it can be improved
			body := types.SnapshotReplicationEvent{
				Etype:  types.SnapshotReplicationEType,
				Key:    key,
				Values: values,
			}
			replSSCh <- body

			// sleep added only to increase time of replication
			// during testing I'm not able to generate enough
			// data to keep replication going on for meaningful
			// duration
			// time.Sleep(100 * time.Millisecond)
		}
		slog.Info("all snapshot events sent")
	}()

	// c.Stream takes a function that returns a boolean.
	// Returning true keeps the stream open; false closes it.
	c.Stream(func(w io.Writer) bool {
		slog.Info("in replication stream")

		// this state is reaches after snapshot replication is complete
		// and liveMutations are not more being replicated
		if isLiveMutationCompleteEventSent && isReplSnapshotCompletionEventSent {
			slog.Info("replication stream complete")
			return false
		}

		// Send an event with a name and data
		select {
		case <-c.Request.Context().Done():
			slog.Error("connection closed before snapshot replication completed")
			return false
		case snapshotEvent, ok := <-replSSCh:
			if !ok {
				replSSCh = nil
				if !isReplSnapshotCompletionEventSent {
					c.SSEvent("message", types.SnapshotReplicationCompleteEvent)
					isReplSnapshotCompletionEventSent = true
				}
				slog.Info("snapshot replication completed")
			} else {
				// snapshotEvent is []interface{key_str, value_interface, version_uint64}
				// after channel is closed, reading from it
				// returns zero value.
				c.SSEvent("message", snapshotEvent)
				slog.Info("sent snapshotevent", "snapshot_event", snapshotEvent)
				return true
			}

		case liveMutationsEvent, ok := <-replLiveMutationChan:
			if !ok {
				replLiveMutationChan = nil
				if !isLiveMutationCompleteEventSent {
					c.SSEvent("message", types.LiveMutationReplicationCompleteEvent)
					isLiveMutationCompleteEventSent = true
				}
				slog.Info("live mutation replication completed")
			} else {

				// update when last live mutation was sent
				lastLiveMutationSentTimeStamp = time.Now()

				// liveMutationsEvent is []interface{key_str, value_interface, version_uint64}
				// after channel is closed, reading from it
				// returns zero value.
				c.SSEvent("message", liveMutationsEvent)
				slog.Info("stream live mutation")
				return true
			}
		}
		return true // Keep streaming
	})

}

func (n *Node) getSnapShot(c *gin.Context) {
	snapshot, cancelHook := storagev2.Snapshot(store.AllKeys)
	defer cancelHook()
	c.JSON(http.StatusOK, gin.H{
		"value": snapshot,
	})
}
