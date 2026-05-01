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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/goyal-aman/distributed_storage_nodes/api_client")

const (
	V1_METADATA_NODE   types.Endpoint = "/v1/metadata/node"
	V1_METDATA_KEY     types.Endpoint = "/v1/metadata/key"
	V1_DATA            types.Endpoint = "/v1/data"
	V1_REPLICA_DATA    types.Endpoint = "/v1/replica/data"
	V1_GOSSIP          types.Endpoint = "v1/gossip"
	V1_SNAPSHOT        types.Endpoint = "/v1/snapshot"
	V1_SNAPSHOT_STREAM types.Endpoint = "/v1/snapshot/stream"
)

// MapToQueryParamStr
// return S where s= "?key1=val1&key2=val2"
// if m is empty then ""
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

func PostRawKeyValue(
	ctx context.Context,
	node types.NodeGossip,
	key string,
	value any,
	version uint64,
) error {
	ctx, span := tracer.Start(ctx, "PostRawKeyValue")
	defer span.End()

	span.SetAttributes(
		attribute.String("dest_node_id", node.Id),
		attribute.String("dest_node_host", node.Host),
	)

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
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	// spanCtx := trace.SpanFromContext(ctx).SpanContext()
	// slog.Info("sender",
	// 	"trace_id", spanCtx.TraceID().String(),
	// 	"span_id", spanCtx.SpanID().String(),
	// )

	resp, perr := client.Do(req)
	if perr != nil {
		slog.Error("err redirect raw post key value", "dest_host", node.Host, "err", perr)
		// return fmt.Errorf("err occured while send init node", err)
		return errors.Join(pkgerr.ErrRedirectPostKeyValue, perr)
	}
	defer resp.Body.Close()

	// Drain the body so the connection can be reused
	_, _ = io.Copy(io.Discard, resp.Body)

	return nil
}

func PostKeyValue(
	ctx context.Context,
	node types.NodeGossip,
	key string,
	value any,
	queryParams map[string]string,
) error {
	ctx, span := tracer.Start(ctx, "PostKeyValue")
	defer span.End()

	payload := map[string]interface{}{
		"key":   key,
		"value": value,
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		node.Host+V1_DATA.String()+MapToQueryParamStr(queryParams),
		helper.ToBytesReader(payload),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	resp, perr := client.Do(req)
	if perr != nil {
		slog.Error("err redirect post key value", "dest_host", node.Host, "err", perr)
		return errors.Join(pkgerr.ErrRedirectPostKeyValue, perr)
	}

	defer resp.Body.Close()

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

func GetKeyValue(
	ctx context.Context,
	node types.NodeGossip,
	queryParams map[string]string,
) (*types.GetDataResponse, error) {
	params := MapToQueryParamStr(queryParams)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		node.Host+V1_DATA.String()+params,
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{}
	resp, gerr := client.Do(req)
	if gerr != nil {
		slog.Error("err redirect get key value", "dest_host", node.Host, "err", gerr)
		return nil, errors.Join(pkgerr.ErrRedirectGetKeyValue, gerr)

	}

	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	body := types.GetDataResponse{}
	if err := json.Unmarshal(respBytes, &body); err != nil {
		return nil, err
	}
	return &body, nil
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
