package apiclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/goyal-aman/distributed-storage-nodes/err"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

const (
	nodeMetaEndpoint           = "/v1/node"
	postAndGetKeyValueEndpoint = "/v1/data"
)

func PostKeyValue(node types.Gossip, key string, value any) error {
	payload := map[string]interface{}{
		"key":   key,
		"value": value,
	}

	resp, perr := http.Post(node.Host+postAndGetKeyValueEndpoint, "application/json", helper.ToBytesReader(payload))
	if perr != nil {
		slog.Error("err redirect post key value", "dest_host", node.Host, "err", perr)
		// return fmt.Errorf("err occured while send init node", err)
		return errors.Join(err.ErrRedirectPostKeyValue, perr)
	}

	respBytes := make([]byte, 0)
	resp.Body.Read(respBytes)
	slog.Debug("send node success")
	return nil
}

func GetKeyValue(node types.Gossip, key string) (map[string]interface{}, error) {

	resp, perr := http.Get(node.Host + postAndGetKeyValueEndpoint + fmt.Sprintf("?key=%s", key))
	if perr != nil {
		slog.Error("err redirect get key value", "dest_host", node.Host, "err", perr)
		return nil, errors.Join(err.ErrRedirectGetKeyValue, perr)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	body := map[string]interface{}{}
	json.Unmarshal(respBytes, &body)
	return body, nil
}

func GetNodeMeta(host string) (map[string]interface{}, error) {
	resp, err := http.Get(host + nodeMetaEndpoint)
	if err != nil {
		return nil, fmt.Errorf("%w error in geting nodemeta", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w err reading response body", err)
	}

	m := map[string]interface{}{}
	json.Unmarshal(respBytes, &m)
	return m, nil

}
