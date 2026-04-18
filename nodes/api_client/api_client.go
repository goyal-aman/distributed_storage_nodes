package apiclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/goyal-aman/distributed-storage-nodes/err"
	pkgerr "github.com/goyal-aman/distributed-storage-nodes/err"
	"github.com/goyal-aman/distributed-storage-nodes/helper"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

const (
	V1_METADATA_NODE   types.Endpoint = "/v1/metadata/node"
	V1_METDATA_KEY     types.Endpoint = "/v1/metadata/key"
	V1_DATA            types.Endpoint = "/v1/data"
	V1_REPLICA_DATA    types.Endpoint = "/v1/replica/data"
	V1_GOSSIP          types.Endpoint = "v1/gossip"
	V1_SNAPSHOT        types.Endpoint = "/v1/snapshot"
	V1_SNAPSHOT_STREAM types.Endpoint = "/v1/snapshot/stream"
)

func MapToQueryParamStr(m map[string]string) string {
	var sb strings.Builder
	len := len(m)
	count := 0
	for key, val := range m {
		if count == 0 {
			sb.WriteString("?")
		}

		sb.WriteString(key)
		sb.WriteString("=")
		sb.WriteString(val)

		if count < len-1 {
			sb.WriteString("&")
		}
		count++
	}
	return sb.String()

}

func PostRawKeyValue(ctx context.Context, node types.NodeGossip, key string, value any, version uint64) error {
	payload := types.HandlePostRawReq{
		Key:     key,
		Value:   value,
		Version: version,
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		node.Host+V1_REPLICA_DATA.String(),
		helper.ToBytesReader(payload),
	)
	if err != nil {
		return err
	}
	// 3. Set required headers
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{}

	_, perr := client.Do(req)
	if perr != nil {
		slog.Error("err redirect raw post key value", "dest_host", node.Host, "err", perr)
		// return fmt.Errorf("err occured while send init node", err)
		return errors.Join(pkgerr.ErrRedirectPostKeyValue, perr)
	}

	return nil
}

func PostKeyValue(node types.NodeGossip, key string, value any, queryParams map[string]string) error {
	payload := map[string]interface{}{
		"key":   key,
		"value": value,
	}

	resp, perr := http.Post(node.Host+V1_DATA.String()+MapToQueryParamStr(queryParams), "application/json", helper.ToBytesReader(payload))
	if perr != nil {
		slog.Error("err redirect post key value", "dest_host", node.Host, "err", perr)
		return errors.Join(pkgerr.ErrRedirectPostKeyValue, perr)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	body := types.PostDataResponse{}
	json.Unmarshal(respBytes, &body)
	if body.IsSuccess {
		return nil
	}
	return errors.New(body.Err)
}

func GetKeyValue(node types.NodeGossip, key string) (map[string]interface{}, error) {

	resp, perr := http.Get(node.Host + V1_DATA.String() + fmt.Sprintf("?key=%s", key))
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
	resp, err := http.Get(host + V1_METADATA_NODE.String())
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

// func RequestSnapshot(host string) (map[string]interface{}, error) {
// 	resp, err := http.Get(host + getSnapShot)
// 	if err != nil {
// 		return nil, fmt.Errorf("%w error in geting snapshot", err)
// 	}
// 	defer resp.Body.Close()

// 	reader := bufio.NewScanner(resp.Body)
// 	for reader.Scan() {
// 		line := reader.Text()

// 		// ignore empty lines
// 		if line == "" {
// 			continue
// 		}

// 		// Only process data lines
// 		if strings.HasPrefix(line, "data: ") {
// 			jsonStr := strings.TrimPrefix(line, "data: ")

// 			fmt.Printf("Received → key=%s, value=%v\n", "Key", jsonStr)
// 		}
// 	}
// 	return nil, nil

// }

func RequestSnapshot(host, sourceid string) (chan types.ReplicationStream, error) {

	resp, gerr := http.Get(host + V1_SNAPSHOT_STREAM.String() + "?sourceid=" + sourceid)
	if gerr != nil {
		slog.Error("err in request replicate", "host", host, "sourceid", sourceid)
		return nil, errors.Join(err.ErrRequestingReplica, gerr)
	}

	streamChan := make(chan types.ReplicationStream, 1)

	go func() {
		defer close(streamChan)
		reader := bufio.NewReader(resp.Body)
		for {
			// Read line by line until a double newline (\n\n) is found
			line, err := reader.ReadBytes('\n')
			if err != nil {
				fmt.Println("Connection closed:", err)
				break
			}

			// SSE lines start with "data: ", "event: ", or "id: "
			if bytes.HasPrefix(line, []byte("data:")) {
				jsonStr := bytes.TrimPrefix(line, []byte("data:"))

				// 1. Partial unmarshal to check the type
				var header struct {
					Etype types.ReplicationEventType `json:"Etype"`
				}

				data := []byte(jsonStr)
				if err := json.Unmarshal(data, &header); err != nil {
					slog.Error("Invalid JSON:", "err", err)
					continue
				}

				slog.Info("replication event recieved", "header", header.Etype, "data", jsonStr)

				// 2. Route to the correct struct based on Etype
				switch header.Etype {
				case types.LiveMutationReplicationEType,
					types.LiveMutationReplicationCompleteEType:
					var event types.LiveMutationReplicationEvent
					if err := json.Unmarshal(data, &event); err == nil {
						streamChan <- types.ReplicationStream{
							EType:                        event.Etype,
							LiveMutationReplicationEvent: event,
						}
					} else {
						slog.Error("error unmarshalling livemutationevent", "type", header.Etype, "err", err, "jsonStr", jsonStr)
					}

				case types.SnapshotReplicationEType,
					types.SnapshotReplicationCompleteEType:
					var event types.SnapshotReplicationEvent
					if err := json.Unmarshal(data, &event); err == nil {
						streamChan <- types.ReplicationStream{
							EType:                    event.Etype,
							SnapshotReplicationEvent: event,
						}
					} else {
						slog.Error("error unmarshalling Snapshotevent", "type", header.Etype, "err", err, "jsonStr", jsonStr)
					}
				default:
					slog.Error("unknown event type")
				}
			}
		}
	}()

	return streamChan, nil
}
