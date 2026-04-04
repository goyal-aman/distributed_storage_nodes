package main

import (
	"bytes"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/goyal-aman/distributed-storage-nodes/cluster/coordinator"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	apiclient "github.com/goyal-aman/distributed-storage-nodes/nodes/api_client"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

const (
	Total_Slots               = uint64((1 << 64) - 1)
	BroadCast_Ticker_Duration = 10 * time.Second
)

var (
	storage = map[string]interface{}{}
)

var (
	fPort          = flag.Int("port", 7770, "port of application, default is 7770")
	fHost          = flag.String("host", "", "host addr of current node. this is mandatory. example 'http://0.0.0.0:7770'")
	fEndOfKeyRange = flag.String("eokr", "", "EndOfKeyRange for the node. this is mandatory. '18446744073709551615' is max value")
	fSeedNodes     = flag.String("seed", "", "comma separated host details of seed node, default is ''; example 'http://0.0.0.0:8080,http://0.0.0.0:8081'")
)

var (
	GVar_PORT int

	// GVar_Host is host address of current node where other nodes will contact on
	GVar_Host          string
	GVar_SeedNodes     []string
	GVar_EndOfKeyRange uint64
)

var (
	updateGossipEndpoint = "/v1/gossip"
	nodeDataEndpoint     = "/v1/data"
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
	Gossip        map[string]types.Gossip
	LastUpdate    time.Time
}

func (n Node) XEndOfKeyRange() uint64 {
	return n.EndOfKeyRange
}

// NewNode
// if seed nodes are provided, then reaches out to seed nodes and get their details
// if successfully receives the details from seed node, then put them as their gossip partner
// if seed nodes are provided but due to some error they are not available
// nodes are without seeds (and gossip)
func NewNode() *Node {
	gossipNodes := map[string]types.Gossip{}
	if len(GVar_SeedNodes) > 0 {
		for _, sNode := range GVar_SeedNodes {
			nodeMeta, err := apiclient.GetNodeMeta(sNode)
			slog.Info("seed node data", "node_meta", nodeMeta)
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
			a := types.Gossip{
				Id:            id,
				Host:          host,
				EndOfKeyRange: endOfKeyRange,
				LastUpdate:    t,
			}
			gossipNodes[id] = a
		}
		slog.Info("seed node found", "seed_node", gossipNodes)
	}

	node := &Node{
		Id:            uuid.New().String(),
		Host:          GVar_Host,
		Gossip:        gossipNodes,
		EndOfKeyRange: GVar_EndOfKeyRange,
		LastUpdate:    time.Now(),
	}
	slog.Info("node created", "node", node)
	return node
}

func main() {
	node := NewNode()
	router := initRouter(node)

	go func() {
		ticker := time.NewTicker(BroadCast_Ticker_Duration)
		for t := range ticker.C {
			node.BroadcastGossip(t)
		}
	}()

	if err := router.Run(fmt.Sprintf("0.0.0.0:%d", GVar_PORT)); err == nil {
		slog.Info("listening on", "port", GVar_PORT)
	}
}

func initRouter(node *Node) *gin.Engine {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	// v1
	routerv1 := router.Group("v1")

	// node
	nodev1 := routerv1.Group("node")

	// v1/node/
	nodev1.GET("", node.nodemeta)
	nodev1.POST("init", node.initNode)

	// v1/data/
	datav1 := routerv1.Group("data")
	datav1.POST("", node.handlePost)
	datav1.GET("", node.handleGet)

	// v1/gossip
	gossipv1 := routerv1.Group("gossip")
	gossipv1.POST("", node.updateGossipNodes)
	gossipv1.GET("", node.getGossipNodes)

	// v1/nodedetail
	nodedetailv1 := routerv1.Group("nodedetail")
	nodedetailv1.GET("", node.nodeForKey)

	return router

}

// nodemeta
// returns present state of node
func (n *Node) nodemeta(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"Id":            n.Id,
		"Host":          n.Host,
		"Gossip":        n.Gossip,
		"EndOfKeyRange": n.EndOfKeyRange,
		"LastUpdate":    n.LastUpdate,
	})
}

// Depricated
func (n *Node) initNode(c *gin.Context) {
	details := map[string]interface{}{}
	if err := c.ShouldBindJSON(&details); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id := details["Id"].(string)
	host := details["Host"].(string)
	endOfKeyRange := uint64(details["EndOfKeyRange"].(float64))

	n.Id = id
	n.Host = host
	n.EndOfKeyRange = endOfKeyRange
	slog.Info("updated node details", "node", n)
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
	payload := n.Gossip

	// update for current node
	// dont do g := n.Gossip[n.Id]; g.LastUpdate=time.Now(); n.Gossip[n.Id]=g
	// because it is possible that n.Gossip may not contain n.Id
	now := time.Now()
	n.Gossip[n.Id] = types.Gossip{
		Id:            n.Id,
		Host:          n.Host,
		EndOfKeyRange: n.EndOfKeyRange,
		LastUpdate:    now,
	}

	gossips := make([]types.Gossip, len(payload))
	idx := 0
	for _, val := range payload {
		gossips[idx] = val
		idx++
	}

	for id, g := range n.Gossip {
		if id == n.Id {
			continue
		}
		slog.Debug("Sending gossips", "from", *fPort, "to", g.Host, "data", gossips)
		sendGossip(g.Host, helper.ToBytesReader(gossips))
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

// updateGossipNodes
// updates gossip using lists of gossips from clients
func (n *Node) updateGossipNodes(c *gin.Context) {
	body := []types.Gossip{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
	}

	beforeUpdateGossip := logIDAndTime2(n.Gossip)
	receviedUpdates := logIDAndTime(body)

	for _, g := range body {
		val, exist := n.Gossip[g.Id]
		if exist {
			val.LastUpdate = helper.MaxTime(val.LastUpdate, g.LastUpdate)
			n.Gossip[g.Id] = val
		} else {
			n.Gossip[g.Id] = g
		}
	}
	afterUpdateGossip := logIDAndTime2(n.Gossip)

	slog.Debug("receive gossip updates", "curr_host", n.Host, "received_updates", receviedUpdates,
		"before_updates", beforeUpdateGossip, "after_updates", afterUpdateGossip)

	c.JSON(http.StatusOK, gin.H{
		"message": n.Gossip,
	})
}

// getGossipNodes
// return the list of gossips
func (n *Node) getGossipNodes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": n.Gossip,
	})
}
func logIDAndTime2(m map[string]types.Gossip) map[string]time.Time {
	m2 := map[string]time.Time{}
	for _, v := range m {
		m2[v.Host] = v.LastUpdate
	}
	return m2

}

func logIDAndTime(g []types.Gossip) map[string]time.Time {
	m := map[string]time.Time{}
	for _, gt := range g {
		m[gt.Host] = gt.LastUpdate
	}
	return m

}

// handlePost
// finds the owner node of key and store value against the key in that node.
// value can be any type.
func (n *Node) handlePost(c *gin.Context) {
	body := map[string]interface{}{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
	}

	ar := helper.MapToArr(n.Gossip)
	sort.Slice(ar, func(a, b int) bool {
		// increasing order in EndOfKeyRange
		return ar[a].EndOfKeyRange < ar[b].EndOfKeyRange
	})

	key := body["key"].(string)
	value := body["value"]

	token := helper.HashKey(key, Total_Slots)
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
		if rerr := apiclient.PostKeyValue(*ownerNode, key, value); rerr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": rerr.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"message": "OK",
				"metadata": map[string]string{
					"redirected":  "true",
					"serviced_by": n.Id,
					"owned_by":    ownerNode.Id,
				},
			})
		}
		return
	}

	slog.Info("handle post", "body", body)
	storage[key] = value
	c.JSON(http.StatusOK, gin.H{
		"message":    "ok",
		"owner_node": ownerNode.Id,
	})
}

func (n *Node) handleGet(c *gin.Context) {
	key := c.Query("key")
	slog.Info("handle get", "key", key)

	ar := helper.MapToArr(n.Gossip)
	sort.Slice(ar, func(a, b int) bool {
		// increasing order in EndOfKeyRange
		return ar[a].EndOfKeyRange < ar[b].EndOfKeyRange
	})

	token := helper.HashKey(key, Total_Slots)
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

	c.JSON(http.StatusOK, gin.H{"message": "ok", "value": storage[key]})
}

// nodedetail return details of node own the 'key'
func (n *Node) nodeForKey(c *gin.Context) {
	key := c.Query("key")
	hash := helper.HashKey(key, coordinator.Total_Slots)

	ar := helper.MapToArr(n.Gossip)
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
