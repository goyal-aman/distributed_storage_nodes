package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

var (
	storage = map[string]interface{}{}
)

var (
	fPORT = flag.Int("port", 7770, "port of application, default is 7770")
)

func init() {
	flag.Parse()
}

func main() {
	PORT := *fPORT

	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	routerv1 := router.Group("v1")
	routerv1.POST("", handlePost)
	routerv1.GET("", handleGet)

	if err := router.Run(fmt.Sprintf("0.0.0.0:%d", PORT)); err == nil {
		slog.Info("listening on", "port", PORT)
	}
}

func handlePost(c *gin.Context) {
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

func handleGet(c *gin.Context) {
	key := c.Query("key")
	slog.Info("handle get", "key", key)
	c.JSON(http.StatusOK, gin.H{"message": "ok", "value": storage[key]})
}
