package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/goyal-aman/distributed-storage-nodes/cluster/coordinator"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

// Depricated: cluster is no longer needed. Each node can start with seed node
var (
	storage = map[string]interface{}{}
)

var cluster *coordinator.Cluster

var (
	initialHost = flag.String("ihost", "", "initial host of the cluster")
	fPORT       = flag.Int("PORT", 8880, "port of application default is 8880")
)

func init() {
	flag.Parse()
}

func main() {
	var (
		PORT = *fPORT
	)

	cluster = coordinator.NewCluster(*initialHost)

	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	router.GET("/info", func(c *gin.Context) {
		nodes := cluster.Info()
		c.JSON(200, gin.H{
			"nodes": nodes,
		})
	})

	routerv1 := router.Group("v1")

	routerv1.POST("node", addnode)
	routerv1.DELETE("node", removenode)

	routerv1.GET("nodedetail", nodedetail)

	if err := router.Run(fmt.Sprintf("0.0.0.0:%d", PORT)); err == nil {
		slog.Info("listening on", "port", PORT)
	}
}

// nodedetail
// return the details which should own the 'key'
func nodedetail(c *gin.Context) {
	key := c.Query("key")
	hash := helper.HashKey(key, coordinator.Total_Slots)

	// find node which handles this hash range
	node, err := cluster.GetNode(hash)
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

func removenode(c *gin.Context) {
	body := map[string]interface{}{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
	}

	slog.Info("remove node", "body", body)

	hostStr := body["Host"].(string)
	endOfKeyRange := uint64(body["EndOfKeyRange"].(float64)) //.(uint64)
	node := types.StorageNode{
		Host:          hostStr,
		EndOfKeyRange: endOfKeyRange,
	}

	err := cluster.RemoveNode(node)

	c.JSON(http.StatusOK, gin.H{"error": err})

}

func addnode(c *gin.Context) {
	body := map[string]interface{}{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "something wrong with body",
			"err":     err.Error(),
		})
	}

	slog.Info("addnode", "body", body)

	idStr := uuid.New().String()
	hostStr := body["Host"].(string)
	endOfKeyRange := uint64(body["EndOfKeyRange"].(float64)) //.(uint64)
	node := types.StorageNode{
		Id:            idStr,
		Host:          hostStr,
		EndOfKeyRange: endOfKeyRange,
	}

	err := cluster.AddNode(node)
	if err != nil {
		slog.Error("err in adding node", "err", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cluster.InitNode(node)
	cluster.BroadCastNode(node)

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func handleGet(c *gin.Context) {
	key := c.Query("key")
	slog.Info("handle get", "key", key)
	c.JSON(http.StatusOK, gin.H{"message": "ok", "value": storage[key]})
}
