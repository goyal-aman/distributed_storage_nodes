package main

import (
	"bytes"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

var (
	storage = map[string]interface{}{}
)

var (
	fPORT = flag.Int("port", 7770, "port of application, default is 7770")
)

var (
	PORT int
)

var (
	updateGossipEndpoint = "/v1/gossip"
)

func init() {
	flag.Parse()

	PORT = *fPORT
}

type Node struct {
	Id            string
	Host          string
	EndOfKeyRange uint64
	Gossip        map[string]types.Gossip
}

func NewNode() *Node {
	return &Node{
		Gossip: map[string]types.Gossip{},
	}
}

func main() {
	node := NewNode()
	router := initRouter(node)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for t := range ticker.C {
			node.BroadcastGossip(t)
		}
	}()

	if err := router.Run(fmt.Sprintf("0.0.0.0:%d", PORT)); err == nil {
		slog.Info("listening on", "port", PORT)
	}
}

func initRouter(node *Node) *gin.Engine {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	routerv1 := router.Group("v1")

	// node
	nodev1 := routerv1.Group("node")
	nodev1.GET("", node.nodemeta)
	nodev1.POST("init", node.initNode)

	// data
	datav1 := routerv1.Group("data")
	datav1.POST("", node.handlePost)
	datav1.GET("", node.handleGet)

	// gossip
	gossipv1 := routerv1.Group("gossip")
	gossipv1.POST("", node.updateGossipNodes)

	return router

}
func (n *Node) nodemeta(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"Id":            n.Id,
		"Host":          n.Host,
		"Gossip":        n.Gossip,
		"EndOfKeyRange": n.EndOfKeyRange,
	})
}
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
func (n *Node) BroadcastGossip(broadCaseTime time.Time) {
	if n.Id == "" {
		return
	}
	payload := n.Gossip

	myGossip := n.Gossip[n.Id]
	myGossip.LastUpdate = time.Now()
	n.Gossip[n.Id] = myGossip

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
		slog.Debug("Sending gossips", "from", *fPORT, "to", g.Host, "data", gossips)
		sendGossip(g.Host, helper.ToBytesReader(gossips))
	}
	slog.Info("Broadcast success")

}

func sendGossip(destHost string, bytesReader *bytes.Reader) {
	resp, err := http.Post(destHost+updateGossipEndpoint, "application/json", bytesReader)
	if err != nil {
		slog.Error("err when sendGossip", "destHost", destHost, err)
		// return fmt.Errorf("err occured while send init node", err)
		return
	}

	respBytes := make([]byte, 0)
	resp.Body.Read(respBytes)
	slog.Info("send gossip success", "destHost", destHost, "status", resp.StatusCode, "resp_body", string(respBytes))

}

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
			val.LastUpdate = maxTime(val.LastUpdate, g.LastUpdate)
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

func maxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		slog.Debug("maxTime", "winner", t1, "a", t1, "b", t2)
		return t1
	}
	slog.Debug("maxTime", "winner", t2, "a", t1, "b", t2)
	return t2
}

func (n *Node) handlePost(c *gin.Context) {
	body := map[string]interface{}{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
	}

	slog.Info("handle post", "body", body)
	storage[body["key"].(string)] = body["value"]
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

func (n *Node) handleGet(c *gin.Context) {
	key := c.Query("key")
	slog.Info("handle get", "key", key)
	c.JSON(http.StatusOK, gin.H{"message": "ok", "value": storage[key]})
}
