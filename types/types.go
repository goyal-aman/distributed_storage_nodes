package types

import (
	"fmt"
	"time"
)

// NoteState
type NodeState string

var (
	JOINING       NodeState = "JOINING"
	BOOTSTRAPPING NodeState = "BOOTSTRAPPING"
	AVAILABLE     NodeState = "AVAILABLE"
)

type NodeGossip struct {
	Id            string
	Host          string
	EndOfKeyRange uint64
	LastUpdate    time.Time
	State         NodeState
	ReplicaCount  int
}

func (g NodeGossip) XEndOfKeyRange() uint64 {
	return g.EndOfKeyRange
}

func (g NodeGossip) XState() NodeState {
	return g.State
}

func (g NodeGossip) Clone() NodeGossip {
	return NodeGossip{
		Id:            g.Id,
		Host:          g.Host,
		EndOfKeyRange: g.EndOfKeyRange,
		LastUpdate:    g.LastUpdate,
		State:         g.State,
		ReplicaCount:  g.ReplicaCount,
	}
}

// StorageNode
type StorageNode struct {
	Id string

	// EndOfKeyRange is the first last key in the keyrange node is responsible for
	EndOfKeyRange uint64

	// Host is ip:port
	Host string

	// even though StorageNode is not being used as this point, I've updated
	// because "Just in case"
	State NodeState
}

func (s *StorageNode) String() string {
	return fmt.Sprintf("{endOfKeyRange:%d, status:%s}", s.EndOfKeyRange, "unknown")
}

func (s StorageNode) XEndOfKeyRange() uint64 {
	return s.EndOfKeyRange
}

func (s StorageNode) XState() NodeState {
	return s.State
}

type StoreEntry struct {
	Value interface{}

	// Version
	// why is it uint64? not sure tbh, it feels like
	// for now, bigger value is better.
	Version uint64

	// IsReplica
	// if true is means the entry was replicated from owner-node
	// to current node store
	IsReplica bool
}

func (e StoreEntry) Clone() StoreEntry {
	return StoreEntry{Value: e.Value, Version: e.Version, IsReplica: e.IsReplica}
}

// replication event
type ReplicationEventType string

var (
	LiveMutationReplicationEType         ReplicationEventType = "LiveMutationEType"
	LiveMutationReplicationCompleteEType ReplicationEventType = "LiveMutationCompleteEType"
	SnapshotReplicationEType             ReplicationEventType = "SnapshotReplicationEType"
	SnapshotReplicationCompleteEType     ReplicationEventType = "SnapshotReplicationCompleteEType"
)

var (
	SnapshotReplicationCompleteEvent = SnapshotReplicationEvent{
		Etype: SnapshotReplicationCompleteEType,
	}

	LiveMutationReplicationCompleteEvent = LiveMutationReplicationEvent{
		Etype: LiveMutationReplicationCompleteEType,
	}
)

type LiveMutationReplicationEvent struct {
	Etype   ReplicationEventType
	Key     string
	Value   interface{}
	Version uint64
}

type SnapshotReplicationEvent struct {
	Etype  ReplicationEventType
	Key    string
	Values []StoreEntry
}

type ReplicationStream struct {
	EType                        ReplicationEventType
	SnapshotReplicationEvent     SnapshotReplicationEvent
	LiveMutationReplicationEvent LiveMutationReplicationEvent
}

type HandlePostRawReq struct {
	Key     string
	Value   interface{}
	Version uint64
}

type Endpoint string

func (e Endpoint) String() string {
	return string(e)
}

type PostDataMetaData struct {
	// omitempty is not present intentionally
	Redirected   bool        `json:"redirected"`
	ServicedBy   string      `json:"serviced_by"`
	OwnedBy      string      `json:"owned_by"`
	ReplicaCount int         `json:"replica_count"`
	Mislaneous   interface{} `json:"mislaneous,omitempty"`
}

type PostDataResponse struct {
	IsSuccess bool              `json:"status"`
	Message   string            `json:"message,omitempty"`
	Metadata  *PostDataMetaData `json:"metadata,omitempty"`
	Err       string            `json:"error,omitempty"`
}

type PostRawDataResponse struct {
	IsSuccess bool              `json:"status"`
	Message   string            `json:"message,omitempty"`
	Metadata  *PostDataMetaData `json:"metadata,omitempty"`
	Err       string            `json:"error,omitempty"`
}

type HasEndOfKeyRange interface {
	XEndOfKeyRange() uint64
}

type HasState interface {
	XState() NodeState
}
