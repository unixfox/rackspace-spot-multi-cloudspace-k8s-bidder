// Package bidder contains the reconcile loop: compare the spot capacity the
// cluster needs with what Rackspace Spot has assigned, place bids in the
// cheapest region that is not cooling down, give up on regions that do not
// fill in time, and release surplus VMs.
package bidder

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/model"
)

// Settings is the planner's slice of the configuration.
type Settings struct {
	Regions       []string // allowed regions, in order of preference
	Families      []string // server class families, in order of preference
	BidPrice      string
	NamePrefix    string
	MinMemory     int64
	MaxVMs        int
	MaxPerRegion  int // 0 = unlimited
	AllocMemRatio float64

	ProvisionTimeout time.Duration
	RegionCooldown   time.Duration
	BootGrace        time.Duration

	ScaleDown            bool
	ScaleDownDelay       time.Duration
	ScaleDownUtilization float64

	CountUnmanaged bool
}

// How long a pool or cloudspace we deleted is ignored while Spot finalizes it.
const deletionGrace = 30 * time.Minute

// Minimum age of an empty managed cloudspace before it is deleted, so a
// cloudspace whose pool creation is not visible yet is not removed.
const emptyCloudspaceGrace = 10 * time.Minute

// ActionKind is a Spot mutation.
type ActionKind string

const (
	CreateCloudspace ActionKind = "create-cloudspace"
	CreatePool       ActionKind = "create-pool"
	UpdatePool       ActionKind = "update-pool"
	DeletePool       ActionKind = "delete-pool"
	DeleteCloudspace ActionKind = "delete-cloudspace"
)

// Action is one Spot mutation decided by Plan.
type Action struct {
	Kind        ActionKind
	Cloudspace  string
	Region      string
	Pool        string
	ServerClass string
	Desired     int
	Bid         string
	Reason      string
}

// Summary describes one planning pass, for logging.
type Summary struct {
	Won         int      // VMs assigned (managed + counted unmanaged)
	WonMemory   int64    // nominal RAM of those VMs
	InFlight    int      // VMs bid for but not assigned yet (managed pools only)
	Booting     int      // won VMs that are not Ready nodes yet
	PendingPods int      // unschedulable spot pods
	PendingVMs  int      // VMs needed to host them
	Unplaceable int      // pending pods larger than a whole VM
	Required    int      // VMs needed beyond Won
	CoolingDown []string // region/serverclass pairs currently skipped
	NoOffering  bool     // capacity was needed but no region/class was eligible
}

// Bidder holds the planner state that Spot does not store for us: when each
// pool started waiting for servers, which regions failed recently, when
// each VM was first seen, and how long spot nodes have been underused.
//
// This state is in memory only. After a restart every timer starts over,
// which errs on the side of waiting longer before failing over or scaling
// down.
type Bidder struct {
	S Settings

	// NewName generates pool names; Spot's console uses UUIDs.
	NewName func() string

	inflightSince map[string]time.Time // pool name -> first seen with won < desired
	cooldownUntil map[string]time.Time // region/serverclass -> skip until
	firstSeen     map[string]time.Time // VM IP -> first seen assigned
	deleted       map[string]time.Time // pool/cloudspace name -> deletion requested at
	lowUtilSince  time.Time            // spot nodes underused since (zero = not underused)
}

// New constructs a Bidder.
func New(s Settings) *Bidder {
	return &Bidder{
		S:             s,
		NewName:       func() string { return uuid.NewString() },
		inflightSince: map[string]time.Time{},
		cooldownUntil: map[string]time.Time{},
		firstSeen:     map[string]time.Time{},
		deleted:       map[string]time.Time{},
	}
}

// pool is a mutable working copy of a model.Pool used while planning.
type pool struct {
	model.Pool
	class model.ServerClass
	known bool // class found in the catalog
}

func (p *pool) inFlight() int { return max(0, p.Desired-p.Won) }

// Plan computes the Spot mutations for one tick. It updates the Bidder's
// timers but performs no I/O.
func (b *Bidder) Plan(now time.Time, sp model.SpotState, cl model.ClusterState) ([]Action, Summary) {
	var acts []Action
	var sum Summary
	bid := b.S.BidPrice

	for name, at := range b.deleted {
		if now.Sub(at) > deletionGrace {
			delete(b.deleted, name)
		}
	}
	for key, until := range b.cooldownUntil {
		if !now.Before(until) {
			delete(b.cooldownUntil, key)
		} else {
			sum.CoolingDown = append(sum.CoolingDown, key)
		}
	}
	sort.Strings(sum.CoolingDown)

	// Working set: managed pools we may change, plus counted unmanaged ones.
	var managed, counted []*pool
	seenPools := map[string]bool{}
	seenIPs := map[string]bool{}
	for _, mp := range sp.Pools {
		if _, gone := b.deleted[mp.Name]; gone {
			continue
		}
		cls, known := sp.ServerClasses[mp.ServerClass]
		p := &pool{Pool: mp, class: cls, known: known}
		if !known {
			slog.Warn("pool uses a server class missing from the catalog; it is not counted",
				"pool", p.Name, "cloudspace", p.Cloudspace, "serverclass", p.ServerClass)
		}
		if p.Managed {
			managed = append(managed, p)
			seenPools[p.Name] = true
			if p.Won < p.Desired {
				if _, ok := b.inflightSince[p.Name]; !ok {
					b.inflightSince[p.Name] = now
				}
			} else {
				delete(b.inflightSince, p.Name)
			}
		}
		if p.Managed || b.S.CountUnmanaged {
			counted = append(counted, p)
			for _, ip := range p.IPs {
				seenIPs[ip] = true
				if _, ok := b.firstSeen[ip]; !ok {
					b.firstSeen[ip] = now
				}
			}
		}
	}
	for name := range b.inflightSince {
		if !seenPools[name] {
			delete(b.inflightSince, name)
		}
	}
	for ip := range b.firstSeen {
		if !seenIPs[ip] {
			delete(b.firstSeen, ip)
		}
	}

	shrink := func(p *pool, desired int, reason string) {
		if desired <= 0 {
			acts = append(acts, Action{Kind: DeletePool, Pool: p.Name, Cloudspace: p.Cloudspace,
				Region: p.Region, ServerClass: p.ServerClass, Reason: reason})
			p.Desired, p.Won = 0, 0
		} else {
			acts = append(acts, Action{Kind: UpdatePool, Pool: p.Name, Cloudspace: p.Cloudspace,
				Region: p.Region, ServerClass: p.ServerClass, Desired: desired, Bid: bid, Reason: reason})
			p.Desired = desired
			p.BidPrice = bid
		}
		if p.Won >= p.Desired {
			delete(b.inflightSince, p.Name)
		}
	}

	// 1. Keep managed pools on the configured bid.
	for _, p := range managed {
		if !samePrice(p.BidPrice, bid) && p.Desired > 0 {
			shrink(p, p.Desired, fmt.Sprintf("bid %s -> %s", p.BidPrice, bid))
		}
	}

	// 2. Region failover: a pool that has waited too long for servers gives
	// up its unfilled part, and its region/class cools down.
	for _, p := range managed {
		since, waiting := b.inflightSince[p.Name]
		if !waiting || now.Sub(since) < b.S.ProvisionTimeout {
			continue
		}
		key := cooldownKey(p.Region, p.ServerClass)
		b.cooldownUntil[key] = now.Add(b.S.RegionCooldown)
		sum.CoolingDown = append(sum.CoolingDown, key)
		shrink(p, p.Won, fmt.Sprintf("only %d/%d servers after %s (bid status %q); cooling down %s for %s",
			p.Won, p.Desired, now.Sub(since).Round(time.Minute), p.BidStatus, key, b.S.RegionCooldown))
	}

	// 3. Current capacity.
	readyIPs := map[string]bool{}
	for _, n := range cl.SpotNodes {
		if n.Ready {
			for _, ip := range n.IPs {
				readyIPs[ip] = true
			}
		}
	}
	perRegion := map[string]int{}
	for _, p := range counted {
		if !p.known {
			continue
		}
		sum.Won += p.Won
		sum.WonMemory += int64(p.Won) * p.class.Resources.Memory
		perRegion[p.Region] += p.Won
		for _, ip := range p.IPs {
			if !readyIPs[ip] && now.Sub(b.firstSeen[ip]) < b.S.BootGrace {
				sum.Booting++
			}
		}
		// Won but no IP reported yet: definitely still booting.
		sum.Booting += max(0, p.Won-len(p.IPs))
	}
	for _, p := range managed {
		if p.known {
			sum.InFlight += p.inFlight()
			perRegion[p.Region] += p.inFlight()
		}
	}

	// 4. Capacity needed beyond what is won.
	offers := b.offerings(now, sp)
	sizing, ok := b.sizingClass(offers, sp)
	if !ok {
		slog.Warn("no server class of the configured families exists in the configured regions",
			"families", b.S.Families, "regions", b.S.Regions)
		return acts, sum
	}
	baselineVMs := 0
	if short := b.S.MinMemory - sum.WonMemory; short > 0 {
		baselineVMs = int(ceilDiv(short, sizing.Resources.Memory))
	}
	sum.PendingPods = len(cl.Pending)
	sum.PendingVMs, sum.Unplaceable = b.packPending(cl, sizing)
	pendingVMs := max(0, sum.PendingVMs-sum.Booting)

	sum.Required = max(baselineVMs, pendingVMs)
	if sum.Won+sum.Required > b.S.MaxVMs {
		sum.Required = max(0, b.S.MaxVMs-sum.Won)
	}

	diff := sum.Required - sum.InFlight
	switch {
	case diff > 0:
		a, placed := b.place(now, diff, offers, sp, managed, perRegion)
		acts = append(acts, a...)
		if placed < diff {
			sum.NoOffering = true
		}
		b.lowUtilSince = time.Time{}
	case diff < 0:
		b.cancel(-diff, managed, shrink)
		b.lowUtilSince = time.Time{}
	default:
		if sum.InFlight == 0 && sum.Required == 0 && sum.PendingVMs == 0 && sum.Booting == 0 {
			b.scaleDown(now, sum, managed, cl, shrink)
		} else {
			b.lowUtilSince = time.Time{}
		}
	}

	// 5. Remove managed cloudspaces left without pools.
	live := map[string]bool{}
	for _, p := range managed {
		if p.Desired > 0 {
			live[p.Cloudspace] = true
		}
	}
	for _, a := range acts {
		if a.Kind == CreatePool {
			live[a.Cloudspace] = true
		}
	}
	for _, cs := range sp.Cloudspaces {
		if _, gone := b.deleted[cs.Name]; gone || !cs.Managed || live[cs.Name] {
			continue
		}
		if now.Sub(cs.Created) < emptyCloudspaceGrace {
			continue
		}
		acts = append(acts, Action{Kind: DeleteCloudspace, Cloudspace: cs.Name, Region: cs.Region,
			Reason: "managed cloudspace has no pools left"})
	}

	return acts, sum
}

// offerings returns the eligible server classes (allowed region, configured
// family, our bid at or above both the market and minimum price, not
// cooling down), cheapest market price first, then by region and family
// preference.
func (b *Bidder) offerings(now time.Time, sp model.SpotState) []model.ServerClass {
	bid, _ := strconv.ParseFloat(b.S.BidPrice, 64)
	var out []model.ServerClass
	for _, sc := range sp.ServerClasses {
		if b.regionRank(sc.Region) < 0 || b.familyRank(sc.Name) < 0 {
			continue
		}
		if sc.MarketPrice > bid || sc.MinBid > bid {
			slog.Debug("offering above our bid", "serverclass", sc.Name,
				"market", sc.MarketPrice, "min_bid", sc.MinBid, "bid", bid)
			continue
		}
		if until, ok := b.cooldownUntil[cooldownKey(sc.Region, sc.Name)]; ok && now.Before(until) {
			continue
		}
		out = append(out, sc)
	}
	sort.Slice(out, func(i, j int) bool { return b.less(out[i], out[j]) })
	return out
}

func (b *Bidder) less(x, y model.ServerClass) bool {
	if x.MarketPrice != y.MarketPrice {
		return x.MarketPrice < y.MarketPrice
	}
	if rx, ry := b.regionRank(x.Region), b.regionRank(y.Region); rx != ry {
		return rx < ry
	}
	if fx, fy := b.familyRank(x.Name), b.familyRank(y.Name); fx != fy {
		return fx < fy
	}
	return x.Name < y.Name
}

// sizingClass is the VM shape used to turn RAM/pod demand into a VM count:
// the best eligible offering, else any class of the preferred family in an
// allowed region (so demand is still computed while everything cools down).
func (b *Bidder) sizingClass(offers []model.ServerClass, sp model.SpotState) (model.ServerClass, bool) {
	if len(offers) > 0 {
		return offers[0], true
	}
	var best model.ServerClass
	found := false
	for _, sc := range sp.ServerClasses {
		if b.regionRank(sc.Region) < 0 || b.familyRank(sc.Name) < 0 {
			continue
		}
		if !found || b.familyRank(sc.Name) < b.familyRank(best.Name) {
			best, found = sc, true
		}
	}
	return best, found
}

// packPending returns how many new VMs of class sc the pending pods need.
// Pods that fit in the free room of an existing Ready spot node are not
// capacity-bound (they are pending for another reason, or about to be
// scheduled on a node that just joined) and are not counted.
func (b *Bidder) packPending(cl model.ClusterState, sc model.ServerClass) (vms, unplaceable int) {
	if len(cl.Pending) == 0 {
		return 0, 0
	}
	var overhead model.Resources
	var bins []model.Resources // free room: existing nodes first, then new VMs
	for _, n := range cl.SpotNodes {
		if !n.Ready {
			continue
		}
		overhead.MilliCPU = max(overhead.MilliCPU, n.DaemonSet.MilliCPU)
		overhead.Memory = max(overhead.Memory, n.DaemonSet.Memory)
		bins = append(bins, n.Allocatable.Sub(n.Requested))
	}
	existing := len(bins)
	newVM := model.Resources{
		MilliCPU: sc.Resources.MilliCPU,
		Memory:   int64(float64(sc.Resources.Memory) * b.S.AllocMemRatio),
	}.Sub(overhead)

	pods := append([]model.Resources(nil), cl.Pending...)
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Memory != pods[j].Memory {
			return pods[i].Memory > pods[j].Memory
		}
		return pods[i].MilliCPU > pods[j].MilliCPU
	})
	for _, pod := range pods {
		if !pod.Fits(newVM) {
			unplaceable++
			continue
		}
		placed := false
		for i := range bins {
			if pod.Fits(bins[i]) {
				bins[i] = bins[i].Sub(pod)
				placed = true
				break
			}
		}
		if !placed {
			bins = append(bins, newVM.Sub(pod))
		}
	}
	return len(bins) - existing, unplaceable
}

// place adds n VMs, one at a time, each to the best offering. An offering
// whose region/class already has an unfilled bid ranks after those that do
// not, so a shortfall of several VMs is spread over regions and the first
// region to fill wins; the others are cancelled once capacity is met.
func (b *Bidder) place(now time.Time, n int, offers []model.ServerClass, sp model.SpotState,
	managed []*pool, perRegion map[string]int) ([]Action, int) {

	poolFor := func(sc model.ServerClass) *pool {
		for _, p := range managed {
			if p.Region == sc.Region && p.ServerClass == sc.Name && p.Desired > 0 {
				return p
			}
		}
		return nil
	}
	csName := func(region string) string { return b.S.NamePrefix + region }
	csExists := map[string]bool{}
	for _, cs := range sp.Cloudspaces {
		if cs.Managed {
			if _, gone := b.deleted[cs.Name]; !gone {
				csExists[cs.Name] = true
			}
		}
	}

	adds := map[string]int{} // serverclass -> VMs added this tick
	waiting := func(sc model.ServerClass) bool {
		if adds[sc.Name] > 0 {
			return true
		}
		p := poolFor(sc)
		return p != nil && p.inFlight() > 0
	}
	placed := 0
	for ; placed < n; placed++ {
		var best *model.ServerClass
		for i := range offers {
			sc := &offers[i]
			if b.S.MaxPerRegion > 0 && perRegion[sc.Region] >= b.S.MaxPerRegion {
				continue
			}
			name := csName(sc.Region)
			if _, gone := b.deleted[name]; gone && !csExists[name] {
				continue // our cloudspace there is still being deleted
			}
			if best == nil || (!waiting(*sc) && waiting(*best)) {
				best = sc
			}
		}
		if best == nil {
			break
		}
		adds[best.Name]++
		perRegion[best.Region]++
	}

	var acts []Action
	for _, sc := range offers { // deterministic order
		k := adds[sc.Name]
		if k == 0 {
			continue
		}
		reason := fmt.Sprintf("need %d more VM(s); market %.3f", k, sc.MarketPrice)
		if p := poolFor(sc); p != nil {
			if p.inFlight() == 0 {
				b.inflightSince[p.Name] = now
			}
			p.Desired += k
			acts = append(acts, Action{Kind: UpdatePool, Pool: p.Name, Cloudspace: p.Cloudspace,
				Region: sc.Region, ServerClass: sc.Name, Desired: p.Desired, Bid: b.S.BidPrice, Reason: reason})
			continue
		}
		cs := csName(sc.Region)
		if !csExists[cs] {
			acts = append(acts, Action{Kind: CreateCloudspace, Cloudspace: cs, Region: sc.Region, Reason: reason})
			csExists[cs] = true
		}
		acts = append(acts, Action{Kind: CreatePool, Pool: b.NewName(), Cloudspace: cs, Region: sc.Region,
			ServerClass: sc.Name, Desired: k, Bid: b.S.BidPrice, Reason: reason})
	}
	return acts, placed
}

// cancel withdraws n unfilled VMs, youngest bids first (no cooldown: the
// region did nothing wrong, the capacity is just no longer needed).
func (b *Bidder) cancel(n int, managed []*pool, shrink func(*pool, int, string)) {
	var waiting []*pool
	for _, p := range managed {
		if p.inFlight() > 0 {
			waiting = append(waiting, p)
		}
	}
	sort.SliceStable(waiting, func(i, j int) bool {
		return b.inflightSince[waiting[i].Name].After(b.inflightSince[waiting[j].Name])
	})
	for _, p := range waiting {
		if n == 0 {
			return
		}
		k := min(n, p.inFlight())
		n -= k
		shrink(p, p.Desired-k, fmt.Sprintf("capacity met elsewhere; withdrawing %d unfilled VM(s)", k))
	}
}

// scaleDown releases one surplus VM (most expensive first) once the spot
// nodes have been underused for ScaleDownDelay and RAM stays >= MinMemory.
func (b *Bidder) scaleDown(now time.Time, sum Summary, managed []*pool, cl model.ClusterState,
	shrink func(*pool, int, string)) {

	if !b.S.ScaleDown {
		return
	}
	var victim *pool
	for _, p := range managed {
		if p.Won == 0 || !p.known {
			continue
		}
		if victim == nil || p.class.MarketPrice > victim.class.MarketPrice ||
			(p.class.MarketPrice == victim.class.MarketPrice && b.regionRank(p.Region) > b.regionRank(victim.Region)) {
			victim = p
		}
	}
	if victim == nil || sum.WonMemory-victim.class.Resources.Memory < b.S.MinMemory {
		b.lowUtilSince = time.Time{}
		return
	}

	var alloc, used model.Resources
	for _, n := range cl.SpotNodes {
		if n.Ready {
			alloc = alloc.Add(n.Allocatable)
			used = used.Add(n.Requested)
		}
	}
	after := alloc.Sub(model.Resources{
		MilliCPU: victim.class.Resources.MilliCPU,
		Memory:   int64(float64(victim.class.Resources.Memory) * b.S.AllocMemRatio),
	})
	u := b.S.ScaleDownUtilization
	if float64(used.Memory) > float64(after.Memory)*u || float64(used.MilliCPU) > float64(after.MilliCPU)*u {
		b.lowUtilSince = time.Time{}
		return
	}
	if b.lowUtilSince.IsZero() {
		b.lowUtilSince = now
	}
	if now.Sub(b.lowUtilSince) < b.S.ScaleDownDelay {
		return
	}
	b.lowUtilSince = time.Time{}
	shrink(victim, victim.Won-1, fmt.Sprintf("spot nodes underused for %s (requests cpu %dm / mem %dMi fit in %d%% of the remaining nodes)",
		b.S.ScaleDownDelay, used.MilliCPU, used.Memory>>20, int(u*100)))
}

func (b *Bidder) regionRank(region string) int {
	for i, r := range b.S.Regions {
		if r == region {
			return i
		}
	}
	return -1
}

// familyRank matches "ch.vs1.large-ord" against families like "ch.vs1.large".
func (b *Bidder) familyRank(class string) int {
	for i, f := range b.S.Families {
		if strings.HasPrefix(class, f+"-") {
			return i
		}
	}
	return -1
}

func cooldownKey(region, class string) string { return region + "/" + class }

func samePrice(a, b string) bool {
	fa, errA := strconv.ParseFloat(strings.TrimPrefix(a, "$"), 64)
	fb, errB := strconv.ParseFloat(strings.TrimPrefix(b, "$"), 64)
	if errA != nil || errB != nil {
		return a == b
	}
	return fa == fb
}

func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }

// SpotReader reads Rackspace Spot state.
type SpotReader interface {
	State(ctx context.Context) (model.SpotState, error)
}

// SpotWriter applies Rackspace Spot mutations.
type SpotWriter interface {
	CreateCloudspace(ctx context.Context, name, region string) error
	CreatePool(ctx context.Context, name, cloudspace, serverClass string, desired int, bid string) error
	UpdatePool(ctx context.Context, name string, desired int, bid string) error
	DeletePool(ctx context.Context, name string) error
	DeleteCloudspace(ctx context.Context, name string) error
}

// ClusterReader reads Kubernetes state.
type ClusterReader interface {
	State(ctx context.Context, now time.Time) (model.ClusterState, error)
}

// Spot is the full Spot dependency of Reconcile.
type Spot interface {
	SpotReader
	SpotWriter
}

// Reconcile performs one tick: read both sides, plan, apply. Applying stops
// at the first failed mutation; the next tick re-plans from fresh state.
func (b *Bidder) Reconcile(ctx context.Context, now time.Time, sp Spot, cl ClusterReader, dryRun bool) (Summary, error) {
	spotState, err := sp.State(ctx)
	if err != nil {
		return Summary{}, err
	}
	clusterState, err := cl.State(ctx, now)
	if err != nil {
		return Summary{}, err
	}

	acts, sum := b.Plan(now, spotState, clusterState)
	for _, a := range acts {
		slog.Info("spot action", "action", a.Kind, "cloudspace", a.Cloudspace, "region", a.Region,
			"pool", a.Pool, "serverclass", a.ServerClass, "desired", a.Desired, "bid", a.Bid,
			"reason", a.Reason, "dry_run", dryRun)
		if dryRun {
			continue
		}
		if err := apply(ctx, sp, a); err != nil {
			return sum, err
		}
		switch a.Kind {
		case DeletePool:
			b.deleted[a.Pool] = now
		case DeleteCloudspace:
			b.deleted[a.Cloudspace] = now
		}
	}
	return sum, nil
}

func apply(ctx context.Context, sp SpotWriter, a Action) error {
	switch a.Kind {
	case CreateCloudspace:
		return sp.CreateCloudspace(ctx, a.Cloudspace, a.Region)
	case CreatePool:
		return sp.CreatePool(ctx, a.Pool, a.Cloudspace, a.ServerClass, a.Desired, a.Bid)
	case UpdatePool:
		return sp.UpdatePool(ctx, a.Pool, a.Desired, a.Bid)
	case DeletePool:
		return sp.DeletePool(ctx, a.Pool)
	case DeleteCloudspace:
		return sp.DeleteCloudspace(ctx, a.Cloudspace)
	}
	return fmt.Errorf("unknown action %q", a.Kind)
}
