// Package model holds the plain data types exchanged between the Spot and
// Kubernetes adapters and the bidder's planning logic. Keeping them free of
// SDK/client types lets the planner be unit-tested with literal fixtures.
package model

import "time"

// Resources is a CPU/memory pair. CPU is in millicores, memory in bytes.
type Resources struct {
	MilliCPU int64
	Memory   int64
}

// Add returns r + o.
func (r Resources) Add(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU + o.MilliCPU, Memory: r.Memory + o.Memory}
}

// Sub returns r - o.
func (r Resources) Sub(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU - o.MilliCPU, Memory: r.Memory - o.Memory}
}

// Fits reports whether r fits inside o on both dimensions.
func (r Resources) Fits(o Resources) bool {
	return r.MilliCPU <= o.MilliCPU && r.Memory <= o.Memory
}

// ServerClass is a Spot server class (flavor) in one region, e.g.
// "ch.vs1.large-ord" in "us-central-ord-1".
type ServerClass struct {
	Name        string
	Region      string
	Resources   Resources // nominal VM size (vCPU, RAM) as advertised by Spot
	MarketPrice float64   // current market price, $/hour
	MinBid      float64   // minimum accepted bid, $/hour
}

// Pool is a Spot VM pool (one server class inside one VM Cloudspace).
type Pool struct {
	Name        string
	Cloudspace  string
	Region      string
	ServerClass string
	Managed     bool // created and owned by the bidder (cloudspace name has our prefix)
	Desired     int
	Won         int // servers actually assigned (running or booting)
	WonBids     int // Spot's wonCount: auctions won, allocated or not
	BidPrice    string
	BidStatus   string
	IPs         []string // IP addresses of the servers currently assigned to this pool
}

// Cloudspace is a Spot VM Cloudspace (bound to exactly one region).
type Cloudspace struct {
	Name    string
	Region  string
	Managed bool
	Created time.Time
}

// SpotState is everything the bidder reads from Rackspace Spot in one tick.
type SpotState struct {
	// ServerClasses indexed by name, for every class in every region (not
	// only the allowed ones) so unmanaged pools can be sized too.
	ServerClasses map[string]ServerClass
	Cloudspaces   []Cloudspace
	Pools         []Pool
}

// SpotNode is a Kubernetes node that runs on a Spot VM.
type SpotNode struct {
	Name        string
	Ready       bool
	IPs         []string
	Allocatable Resources
	Requested   Resources // sum of requests of all non-terminated pods on the node
	DaemonSet   Resources // part of Requested that comes from DaemonSet pods
}

// ClusterState is everything the bidder reads from Kubernetes in one tick.
type ClusterState struct {
	// Pending holds the resource requests of pods that tolerate the spot
	// taint, could run on a spot node, and are currently Unschedulable.
	Pending   []Resources
	SpotNodes []SpotNode
}
