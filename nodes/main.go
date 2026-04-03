package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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
	id   string
)

func init() {
	flag.Parse()

	PORT = *fPORT
	id = uuid.New().String()
}

type Node struct {
	Id            string
	Host          string
	EndOfKeyRange uint64
	Gossip        map[string]types.Gossip
}

func NewNode() *Node {
	return &Node{
		Gossip: make(map[string]types.Gossip),
	}
}

func main() {
	node := NewNode()
	router := initRouter(node)

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

// func (n *Node) BroadcastGossip() {

// 	payload := map[string]types.Gossip{
// 		id: types.Gossip{
// 			Id:   id,
// 			Host: fmt.Sprintf("http://0.0.0.0:%d", PORT),
// 		},
// 	}

// 	for id, g := range gossipNode {

// 	}

// }
// func sendGossip() {

// }

func (n *Node) updateGossipNodes(c *gin.Context) {
	body := []types.Gossip{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
	}

	for _, g := range body {
		val, exist := n.Gossip[g.Id]
		if exist {
			val.LastUpdate = maxTime(val.LastUpdate, g.LastUpdate)
		} else {
			n.Gossip[g.Id] = g
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": n.Gossip,
	})
}

func maxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
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
