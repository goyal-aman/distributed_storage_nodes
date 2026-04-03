package types

import "time"

type Gossip struct {
	Id            string
	Host          string
	EndOfKeyRange uint64
	LastUpdate    time.Time
}
