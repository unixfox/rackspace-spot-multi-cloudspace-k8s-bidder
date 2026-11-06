package bidder

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/model"
)

const gi = int64(1) << 30

var t0 = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func settings() Settings {
	return Settings{
		Regions:              []string{"uk-lon-1", "us-east-iad-1", "us-central-ord-1", "us-central-dfw-1"},
		Families:             []string{"ch.vs1.large"},
		BidPrice:             "0.01",
		NamePrefix:           "spot-bidder-",
		MinMemory:            16 * gi,
		MaxVMs:               6,
		AllocMemRatio:        0.9,
		ProvisionTimeout:     30 * time.Minute,
		RegionCooldown:       2 * time.Hour,
		BootGrace:            20 * time.Minute,
		ScaleDown:            true,
		ScaleDownDelay:       30 * time.Minute,
		ScaleDownUtilization: 0.6,
		CountUnmanaged:       true,
	}
}

func newBidder(s Settings) *Bidder {
	b := New(s)
	n := 0
	b.NewName = func() string { n++; return fmt.Sprintf("pool-%d", n) }
	return b
}

func large(region, suffix string, market float64) model.ServerClass {
	return model.ServerClass{
		Name:        "ch.vs1.large-" + suffix,
		Region:      region,
		Resources:   model.Resources{MilliCPU: 4000, Memory: 8 * gi},
		MarketPrice: market,
		MinBid:      0.01,
	}
}

// catalog mirrors real prices seen on 2026-11-05: London cheapest, Sydney
// and Hong Kong not allowed, plus a class of another family.
func catalog() map[string]model.ServerClass {
	m := map[string]model.ServerClass{}
	for _, sc := range []model.ServerClass{
		large("uk-lon-1", "lon", 0.001),
		large("us-east-iad-1", "iad", 0.005),
		large("us-central-ord-1", "ord", 0.010),
		large("us-central-dfw-1", "dfw", 0.010),
		large("aus-syd-1", "syd", 0.001),
		large("hkg-hkg-1", "hkg", 0.001),
		{Name: "gp.vs1.large-lon", Region: "uk-lon-1", Resources: model.Resources{MilliCPU: 4000, Memory: 16 * gi}, MarketPrice: 0.002, MinBid: 0.01},
	} {
		m[sc.Name] = sc
	}
	return m
}

func kinds(acts []Action) []string {
	var out []string
	for _, a := range acts {
		out = append(out, fmt.Sprintf("%s %s %s %d", a.Kind, a.Cloudspace, a.ServerClass, a.Desired))
	}
	return out
}

func readyNode(name, ip string, used int64) model.SpotNode {
	return model.SpotNode{
		Name: name, Ready: true, IPs: []string{ip},
		Allocatable: model.Resources{MilliCPU: 4000, Memory: 7 * gi},
		Requested:   model.Resources{MilliCPU: 500, Memory: used},
	}
}

func TestEmptyStartSpreadsBaselineOverCheapestRegions(t *testing.T) {
	b := newBidder(settings())
	acts, sum := b.Plan(t0, model.SpotState{ServerClasses: catalog()}, model.ClusterState{})

	want := []string{
		"create-cloudspace spot-bidder-uk-lon-1  0",
		"create-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 1",
		"create-cloudspace spot-bidder-us-east-iad-1  0",
		"create-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 1",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
	if sum.Required != 2 {
		t.Fatalf("required = %d, want 2", sum.Required)
	}
	for _, a := range acts {
		if a.Kind == CreatePool && a.Bid != "0.01" {
			t.Fatalf("bid = %q, want 0.01", a.Bid)
		}
	}
}

func TestInFlightBidsAreNotDuplicated(t *testing.T) {
	b := newBidder(settings())
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces: []model.Cloudspace{
			{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0},
			{Name: "spot-bidder-us-east-iad-1", Region: "us-east-iad-1", Managed: true, Created: t0},
		},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 1, BidPrice: "0.01"},
			{Name: "b", Cloudspace: "spot-bidder-us-east-iad-1", Region: "us-east-iad-1", ServerClass: "ch.vs1.large-iad", Managed: true, Desired: 1, BidPrice: "0.01"},
		},
	}
	acts, sum := b.Plan(t0.Add(10*time.Minute), sp, model.ClusterState{})
	if len(acts) != 0 {
		t.Fatalf("expected no actions while bids are in flight, got %q", kinds(acts))
	}
	if sum.InFlight != 2 {
		t.Fatalf("in flight = %d, want 2", sum.InFlight)
	}
}

func TestProvisionTimeoutFailsOverToAnotherRegion(t *testing.T) {
	b := newBidder(settings())
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces: []model.Cloudspace{
			{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0},
			{Name: "spot-bidder-us-east-iad-1", Region: "us-east-iad-1", Managed: true, Created: t0},
		},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 1, BidPrice: "0.01", BidStatus: "Lost"},
			{Name: "b", Cloudspace: "spot-bidder-us-east-iad-1", Region: "us-east-iad-1", ServerClass: "ch.vs1.large-iad", Managed: true, Desired: 1, Won: 1, BidPrice: "0.01", IPs: []string{"10.0.0.2"}},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n2", "10.0.0.2", 1*gi)}}

	b.Plan(t0, sp, cl) // first sighting starts London's timer
	acts, sum := b.Plan(t0.Add(31*time.Minute), sp, cl)

	want := []string{
		"delete-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 0",
		// iad (market 0.005) is the next cheapest and has no unfilled bid.
		"update-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 2",
		"delete-cloudspace spot-bidder-uk-lon-1  0",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
	if len(sum.CoolingDown) != 1 || sum.CoolingDown[0] != "uk-lon-1/ch.vs1.large-lon" {
		t.Fatalf("cooling down = %v", sum.CoolingDown)
	}

	// London stays excluded until the cooldown ends.
	if offers := b.offerings(t0.Add(2*time.Hour), sp); offers[0].Name == "ch.vs1.large-lon" {
		t.Fatalf("london offered during cooldown")
	}
	if offers := b.offerings(t0.Add(31*time.Minute+2*time.Hour), sp); offers[0].Name != "ch.vs1.large-lon" {
		t.Fatalf("london not offered after cooldown, got %s", offers[0].Name)
	}
}

func TestPartialPoolKeepsWonServersOnTimeout(t *testing.T) {
	b := newBidder(settings())
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   []model.Cloudspace{{Name: "spot-bidder-us-central-ord-1", Region: "us-central-ord-1", Managed: true, Created: t0}},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-us-central-ord-1", Region: "us-central-ord-1", ServerClass: "ch.vs1.large-ord", Managed: true, Desired: 2, Won: 1, BidPrice: "0.01", BidStatus: "Partial", IPs: []string{"10.0.0.1"}},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", 1*gi)}}
	b.Plan(t0, sp, cl)
	acts, _ := b.Plan(t0.Add(30*time.Minute), sp, cl)

	want := []string{
		"update-pool spot-bidder-us-central-ord-1 ch.vs1.large-ord 1",
		"create-cloudspace spot-bidder-uk-lon-1  0",
		"create-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 1",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
}

func TestOfferingsAboveBidAreSkipped(t *testing.T) {
	b := newBidder(settings())
	cat := catalog()
	for _, n := range []string{"ch.vs1.large-lon", "ch.vs1.large-iad"} {
		sc := cat[n]
		sc.MarketPrice = 0.02
		cat[n] = sc
	}
	offers := b.offerings(t0, model.SpotState{ServerClasses: cat})
	var names []string
	for _, o := range offers {
		names = append(names, o.Name)
	}
	// ord and dfw tie on price (market == bid is still eligible): region order breaks it.
	if fmt.Sprint(names) != "[ch.vs1.large-ord ch.vs1.large-dfw]" {
		t.Fatalf("offers = %v", names)
	}
}

// Two won VMs meet MIN_MEMORY; spot nodes are full and two 1Gi pods are
// Unschedulable → one more VM.
func TestPendingPodsAddCapacityUnlessAVMIsBooting(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   []model.Cloudspace{{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0}},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 2, BidPrice: "0.01", IPs: []string{"10.0.0.1", "10.0.0.2"}},
		},
	}
	cl := model.ClusterState{
		SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", 7*gi), readyNode("n2", "10.0.0.2", 7*gi)},
		Pending:   []model.Resources{{MilliCPU: 100, Memory: gi}, {MilliCPU: 100, Memory: gi}},
	}

	b := newBidder(settings())
	acts, sum := b.Plan(t0, sp, cl)
	if sum.PendingVMs != 1 {
		t.Fatalf("pending VMs = %d, want 1", sum.PendingVMs)
	}
	want := []string{"update-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 3"}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}

	// Next tick: the third VM is won but its node has not joined yet; it
	// absorbs the pending pods, so nothing more is bid.
	sp.Pools[0].Desired, sp.Pools[0].Won = 3, 3
	sp.Pools[0].IPs = append(sp.Pools[0].IPs, "10.0.0.3")
	acts, sum = b.Plan(t0.Add(5*time.Minute), sp, cl)
	if len(acts) != 0 || sum.Booting != 1 {
		t.Fatalf("expected no action with 1 booting VM, got %q (booting %d)", kinds(acts), sum.Booting)
	}
}

func TestPendingPodThatFitsAnExistingNodeIsIgnored(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   []model.Cloudspace{{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0}},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 2, BidPrice: "0.01", IPs: []string{"10.0.0.1", "10.0.0.2"}},
		},
	}
	cl := model.ClusterState{
		SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", 2*gi), readyNode("n2", "10.0.0.2", 7*gi)},
		Pending:   []model.Resources{{MilliCPU: 100, Memory: gi}},
	}
	acts, sum := newBidder(settings()).Plan(t0, sp, cl)
	if len(acts) != 0 || sum.PendingVMs != 0 {
		t.Fatalf("expected no action, got %q (pending VMs %d)", kinds(acts), sum.PendingVMs)
	}
}

func TestUnfilledBidsAreWithdrawnOnceCapacityIsMet(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces: []model.Cloudspace{
			{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0},
			{Name: "spot-bidder-us-east-iad-1", Region: "us-east-iad-1", Managed: true, Created: t0},
		},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 2, BidPrice: "0.01", IPs: []string{"10.0.0.1", "10.0.0.2"}},
			{Name: "b", Cloudspace: "spot-bidder-us-east-iad-1", Region: "us-east-iad-1", ServerClass: "ch.vs1.large-iad", Managed: true, Desired: 1, BidPrice: "0.01"},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", gi), readyNode("n2", "10.0.0.2", gi)}}
	acts, _ := newBidder(settings()).Plan(t0.Add(time.Hour), sp, cl)
	want := []string{
		"delete-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 0",
		"delete-cloudspace spot-bidder-us-east-iad-1  0",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
}

func TestUnmanagedWonServersCountButAreNeverTouched(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces: []model.Cloudspace{
			{Name: "ordcl1", Region: "us-central-ord-1", Created: t0},
			{Name: "dfwcl1", Region: "us-central-dfw-1", Created: t0},
		},
		Pools: []model.Pool{
			{Name: "u1", Cloudspace: "ordcl1", Region: "us-central-ord-1", ServerClass: "ch.vs1.large-ord", Desired: 2, Won: 1, BidPrice: "0.01", IPs: []string{"10.0.0.1"}},
			{Name: "u2", Cloudspace: "dfwcl1", Region: "us-central-dfw-1", ServerClass: "ch.vs1.large-dfw", Desired: 2, Won: 0, BidPrice: "0.01"},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", gi)}}
	acts, sum := newBidder(settings()).Plan(t0.Add(time.Hour), sp, cl)
	want := []string{
		"create-cloudspace spot-bidder-uk-lon-1  0",
		"create-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 1",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
	if sum.Won != 1 {
		t.Fatalf("won = %d, want 1", sum.Won)
	}
}

func TestScaleDownAfterDelayKeepsMinMemory(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces: []model.Cloudspace{
			{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0},
			{Name: "spot-bidder-us-central-ord-1", Region: "us-central-ord-1", Managed: true, Created: t0},
		},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 2, BidPrice: "0.01", IPs: []string{"10.0.0.1", "10.0.0.2"}},
			{Name: "b", Cloudspace: "spot-bidder-us-central-ord-1", Region: "us-central-ord-1", ServerClass: "ch.vs1.large-ord", Managed: true, Desired: 1, Won: 1, BidPrice: "0.01", IPs: []string{"10.0.0.3"}},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{
		readyNode("n1", "10.0.0.1", gi), readyNode("n2", "10.0.0.2", gi), readyNode("n3", "10.0.0.3", gi),
	}}
	b := newBidder(settings())
	b.S.BootGrace = 0 // IPs are first seen during this test; they are Ready anyway

	if acts, _ := b.Plan(t0, sp, cl); len(acts) != 0 {
		t.Fatalf("scaled down before delay: %q", kinds(acts))
	}
	acts, _ := b.Plan(t0.Add(31*time.Minute), sp, cl)
	// Ord is the most expensive market; its pool only has one VM → deleted.
	want := []string{
		"delete-pool spot-bidder-us-central-ord-1 ch.vs1.large-ord 0",
		"delete-cloudspace spot-bidder-us-central-ord-1  0",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}

	// At exactly MIN_MEMORY nothing more is released.
	sp.Pools = sp.Pools[:1]
	sp.Cloudspaces = sp.Cloudspaces[:1]
	cl.SpotNodes = cl.SpotNodes[:2]
	b2 := newBidder(settings())
	b2.Plan(t0, sp, cl)
	if acts, _ := b2.Plan(t0.Add(time.Hour), sp, cl); len(acts) != 0 {
		t.Fatalf("scaled below MIN_MEMORY: %q", kinds(acts))
	}
}

func TestBidDriftIsCorrected(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   []model.Cloudspace{{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0}},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 2, BidPrice: "0.05", IPs: []string{"10.0.0.1", "10.0.0.2"}},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", 5*gi), readyNode("n2", "10.0.0.2", 5*gi)}}
	acts, _ := newBidder(settings()).Plan(t0, sp, cl)
	if len(acts) != 1 || acts[0].Kind != UpdatePool || acts[0].Bid != "0.01" || acts[0].Desired != 2 {
		t.Fatalf("actions = %q", kinds(acts))
	}
}

func TestEmptyManagedCloudspaceIsDeleted(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces: []model.Cloudspace{
			{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0},
			{Name: "spot-bidder-us-central-dfw-1", Region: "us-central-dfw-1", Managed: true, Created: t0},
			{Name: "manual", Region: "us-central-dfw-1", Created: t0},
		},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 2, BidPrice: "0.01", IPs: []string{"10.0.0.1", "10.0.0.2"}},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", 5*gi), readyNode("n2", "10.0.0.2", 5*gi)}}
	acts, _ := newBidder(settings()).Plan(t0.Add(time.Hour), sp, cl)
	want := []string{"delete-cloudspace spot-bidder-us-central-dfw-1  0"}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
}

func TestMaxVMsCapsDemand(t *testing.T) {
	s := settings()
	s.MaxVMs = 1
	acts, sum := newBidder(s).Plan(t0, model.SpotState{ServerClasses: catalog()}, model.ClusterState{})
	if sum.Required != 1 || len(acts) != 2 {
		t.Fatalf("required %d, actions %q", sum.Required, kinds(acts))
	}
}

// fakeSpot records applied actions and fails on a chosen one.
type fakeSpot struct {
	state   model.SpotState
	applied []string
	failOn  ActionKind
}

func (f *fakeSpot) State(context.Context) (model.SpotState, error) { return f.state, nil }
func (f *fakeSpot) rec(k ActionKind, s string) error {
	if k == f.failOn {
		return fmt.Errorf("boom")
	}
	f.applied = append(f.applied, string(k)+" "+s)
	return nil
}
func (f *fakeSpot) CreateCloudspace(_ context.Context, name, _ string) error {
	return f.rec(CreateCloudspace, name)
}
func (f *fakeSpot) CreatePool(_ context.Context, name, _, _ string, _ int, _ string) error {
	return f.rec(CreatePool, name)
}
func (f *fakeSpot) UpdatePool(_ context.Context, name string, _ int, _ string) error {
	return f.rec(UpdatePool, name)
}
func (f *fakeSpot) DeletePool(_ context.Context, name string) error {
	return f.rec(DeletePool, name)
}
func (f *fakeSpot) DeleteCloudspace(_ context.Context, name string) error {
	return f.rec(DeleteCloudspace, name)
}

type fakeCluster struct{ state model.ClusterState }

func (f fakeCluster) State(context.Context, time.Time) (model.ClusterState, error) {
	return f.state, nil
}

func TestReconcileAppliesAndStopsOnError(t *testing.T) {
	sp := &fakeSpot{state: model.SpotState{ServerClasses: catalog()}}
	if _, err := newBidder(settings()).Reconcile(context.Background(), t0, sp, fakeCluster{}, false); err != nil {
		t.Fatal(err)
	}
	if len(sp.applied) != 4 {
		t.Fatalf("applied = %v", sp.applied)
	}

	sp = &fakeSpot{state: model.SpotState{ServerClasses: catalog()}, failOn: CreatePool}
	if _, err := newBidder(settings()).Reconcile(context.Background(), t0, sp, fakeCluster{}, false); err == nil {
		t.Fatal("expected error")
	}
	if len(sp.applied) != 1 {
		t.Fatalf("applied after failure = %v", sp.applied)
	}

	sp = &fakeSpot{state: model.SpotState{ServerClasses: catalog()}}
	if _, err := newBidder(settings()).Reconcile(context.Background(), t0, sp, fakeCluster{}, true); err != nil {
		t.Fatal(err)
	}
	if len(sp.applied) != 0 {
		t.Fatalf("dry run applied %v", sp.applied)
	}
}

// With one VM per region, a pool asking for 2 (1 won) gives up its unfilled
// bid and the second VM is bid for in the next region instead.
func TestMaxPerRegionSpreadsOneVMPerRegion(t *testing.T) {
	s := settings()
	s.MaxPerRegion = 1
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   []model.Cloudspace{{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0}},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true, Desired: 2, Won: 1, BidPrice: "0.01", IPs: []string{"10.0.0.1"}},
		},
	}
	cl := model.ClusterState{SpotNodes: []model.SpotNode{readyNode("n1", "10.0.0.1", gi)}}
	acts, _ := newBidder(s).Plan(t0, sp, cl)
	want := []string{
		"update-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 1",
		"create-cloudspace spot-bidder-us-east-iad-1  0",
		"create-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 1",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}

	// From empty, the 16Gi baseline lands as one VM in each of the two
	// cheapest regions.
	acts, _ = newBidder(s).Plan(t0, model.SpotState{ServerClasses: catalog()}, model.ClusterState{})
	want = []string{
		"create-cloudspace spot-bidder-uk-lon-1  0",
		"create-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 1",
		"create-cloudspace spot-bidder-us-east-iad-1  0",
		"create-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 1",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions from empty:\n got %q\nwant %q", got, want)
	}
}

// Night of 2026-11-05: London reported bidStatus "Fulfilled" / wonCount 2
// but stayed "Provisioning/Reserving" for 8h with no server assigned. Won
// bids without servers must count as in flight, time out, and fail over.
func TestWonBidsWithoutServersTimeOutAndFailOver(t *testing.T) {
	s := settings()
	s.MaxPerRegion = 1
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   []model.Cloudspace{{Name: "spot-bidder-uk-lon-1", Region: "uk-lon-1", Managed: true, Created: t0}},
		Pools: []model.Pool{
			{Name: "a", Cloudspace: "spot-bidder-uk-lon-1", Region: "uk-lon-1", ServerClass: "ch.vs1.large-lon", Managed: true,
				Desired: 2, Won: 0, WonBids: 2, BidPrice: "0.01", BidStatus: "Fulfilled"},
		},
	}
	b := newBidder(s)

	// Not capacity: the cap trims London to 1 unfilled VM and the other
	// is bid for in iad right away.
	acts, sum := b.Plan(t0, sp, model.ClusterState{})
	if sum.Won != 0 || sum.Booting != 0 {
		t.Fatalf("won %d booting %d, want 0/0", sum.Won, sum.Booting)
	}
	want := []string{
		"update-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 1",
		"create-cloudspace spot-bidder-us-east-iad-1  0",
		"create-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 1",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}

	// Still no server after the timeout: London is dropped and cools down.
	sp.Pools[0].Desired = 1
	acts, sum = b.Plan(t0.Add(31*time.Minute), sp, model.ClusterState{})
	if got := kinds(acts); len(got) == 0 || got[0] != "delete-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 0" {
		t.Fatalf("actions = %q", got)
	}
	if len(sum.CoolingDown) != 1 || sum.CoolingDown[0] != "uk-lon-1/ch.vs1.large-lon" {
		t.Fatalf("cooling down = %v", sum.CoolingDown)
	}
}

func raceSettings() Settings {
	s := settings()
	s.Race = true
	s.MaxPerRegion = 1
	s.ProvisionTimeout = 0
	return s
}

func racePool(region, suffix string, desired, won int, ips ...string) model.Pool {
	return model.Pool{Name: "p-" + suffix, Cloudspace: "spot-bidder-" + region, Region: region,
		ServerClass: "ch.vs1.large-" + suffix, Managed: true, Desired: desired, Won: won,
		WonBids: desired, BidPrice: "0.01", IPs: ips}
}

func raceCloudspaces(regions ...string) []model.Cloudspace {
	var out []model.Cloudspace
	for _, r := range regions {
		out = append(out, model.Cloudspace{Name: "spot-bidder-" + r, Region: r, Managed: true, Created: t0})
	}
	return out
}

// From empty, every allowed region (and not Sydney/Hong Kong) bids for one VM.
func TestRaceBidsInEveryRegionAtOnce(t *testing.T) {
	acts, sum := newBidder(raceSettings()).Plan(t0, model.SpotState{ServerClasses: catalog()}, model.ClusterState{})
	var pools []string
	for _, a := range acts {
		if a.Kind == CreatePool {
			pools = append(pools, fmt.Sprintf("%s=%d", a.ServerClass, a.Desired))
		}
	}
	want := "[ch.vs1.large-lon=1 ch.vs1.large-iad=1 ch.vs1.large-ord=1 ch.vs1.large-dfw=1]"
	if fmt.Sprint(pools) != want {
		t.Fatalf("pools = %v, want %s", pools, want)
	}
	if sum.Required != 2 || sum.NoOffering {
		t.Fatalf("required %d, no offering %v", sum.Required, sum.NoOffering)
	}
}

// Two regions assign servers: the two still-unfilled bids are withdrawn.
func TestRaceWithdrawsLosersOnceCapacityIsAssigned(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   raceCloudspaces("uk-lon-1", "us-east-iad-1", "us-central-ord-1", "us-central-dfw-1"),
		Pools: []model.Pool{
			racePool("uk-lon-1", "lon", 1, 0), // won bid stuck in "Reserving"
			racePool("us-east-iad-1", "iad", 1, 1, "10.0.0.1"),
			racePool("us-central-ord-1", "ord", 1, 1, "10.0.0.2"),
			racePool("us-central-dfw-1", "dfw", 1, 0),
		},
	}
	acts, _ := newBidder(raceSettings()).Plan(t0.Add(time.Hour), sp, model.ClusterState{})
	want := []string{
		"delete-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 0",
		"delete-pool spot-bidder-us-central-dfw-1 ch.vs1.large-dfw 0",
		"delete-cloudspace spot-bidder-uk-lon-1  0",
		"delete-cloudspace spot-bidder-us-central-dfw-1  0",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("actions:\n got %q\nwant %q", got, want)
	}
}

// One server assigned, one still needed: the other regions keep racing,
// the winner's region is full (MAX_VMS_PER_REGION=1), and with
// PROVISION_TIMEOUT=0 nothing ever times out.
func TestRaceKeepsRacingWhileShortAndNeverTimesOut(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   raceCloudspaces("uk-lon-1", "us-east-iad-1", "us-central-ord-1", "us-central-dfw-1"),
		Pools: []model.Pool{
			racePool("uk-lon-1", "lon", 1, 0),
			racePool("us-east-iad-1", "iad", 1, 1, "10.0.0.1"),
			racePool("us-central-ord-1", "ord", 1, 0),
			racePool("us-central-dfw-1", "dfw", 1, 0),
		},
	}
	b := newBidder(raceSettings())
	for _, at := range []time.Duration{0, 10 * time.Hour} {
		acts, sum := b.Plan(t0.Add(at), sp, model.ClusterState{})
		if len(acts) != 0 {
			t.Fatalf("at +%s: unexpected actions %q", at, kinds(acts))
		}
		if sum.Won != 1 || sum.InFlight != 3 || len(sum.CoolingDown) != 0 {
			t.Fatalf("at +%s: won %d in flight %d cooling %v", at, sum.Won, sum.InFlight, sum.CoolingDown)
		}
	}
}

// Without a per-region cap each region races for the whole shortfall.
func TestRaceWithoutCapBidsWholeShortfallPerRegion(t *testing.T) {
	s := raceSettings()
	s.MaxPerRegion = 0
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   raceCloudspaces("uk-lon-1"),
		Pools:         []model.Pool{racePool("uk-lon-1", "lon", 1, 0)},
	}
	acts, _ := newBidder(s).Plan(t0, sp, model.ClusterState{})
	got := kinds(acts)
	if len(got) == 0 || got[0] != "update-pool spot-bidder-uk-lon-1 ch.vs1.large-lon 2" {
		t.Fatalf("actions = %q", got)
	}
	if n := len(got); n != 7 { // lon update + 3 × (cloudspace + pool)
		t.Fatalf("got %d actions: %q", n, got)
	}
}

func consolidateSettings() Settings {
	s := raceSettings()
	s.MaxPerRegion = 0
	s.Consolidate = true
	return s
}

func nodes(ips ...string) model.ClusterState {
	var cl model.ClusterState
	for i, ip := range ips {
		cl.SpotNodes = append(cl.SpotNodes, readyNode(fmt.Sprintf("n%d", i), ip, gi))
	}
	return cl
}

// Every region races to host the whole 16Gi (2 VMs) on its own.
func TestConsolidateRacesWholeTargetPerRegion(t *testing.T) {
	acts, _ := newBidder(consolidateSettings()).Plan(t0, model.SpotState{ServerClasses: catalog()}, model.ClusterState{})
	var pools []string
	for _, a := range acts {
		if a.Kind == CreatePool {
			pools = append(pools, fmt.Sprintf("%s=%d", a.ServerClass, a.Desired))
		}
	}
	want := "[ch.vs1.large-lon=2 ch.vs1.large-iad=2 ch.vs1.large-ord=2 ch.vs1.large-dfw=2]"
	if fmt.Sprint(pools) != want {
		t.Fatalf("pools = %v, want %s", pools, want)
	}
}

// Capacity is met but split over London and iad: keep racing for a region
// that can hold both, release nothing yet.
func TestConsolidateKeepsRacingWhileSplit(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   raceCloudspaces("uk-lon-1", "us-east-iad-1", "us-central-ord-1", "us-central-dfw-1"),
		Pools: []model.Pool{
			racePool("uk-lon-1", "lon", 2, 1, "10.0.0.1"),
			racePool("us-east-iad-1", "iad", 2, 1, "10.0.0.2"),
			racePool("us-central-ord-1", "ord", 2, 0),
			racePool("us-central-dfw-1", "dfw", 2, 0),
		},
	}
	acts, sum := newBidder(consolidateSettings()).Plan(t0, sp, nodes("10.0.0.1", "10.0.0.2"))
	if len(acts) != 0 || sum.Won != 2 || sum.Required != 0 {
		t.Fatalf("actions %q, won %d, required %d", kinds(acts), sum.Won, sum.Required)
	}
}

// London now holds both VMs. While its second VM is still booting, only
// the unfilled bids are withdrawn; once it is Ready, iad's VM is released.
func TestConsolidateMovesToHomeRegionOnceReady(t *testing.T) {
	sp := model.SpotState{
		ServerClasses: catalog(),
		Cloudspaces:   raceCloudspaces("uk-lon-1", "us-east-iad-1", "us-central-ord-1", "us-central-dfw-1"),
		Pools: []model.Pool{
			racePool("uk-lon-1", "lon", 2, 2, "10.0.0.1", "10.0.0.3"),
			racePool("us-east-iad-1", "iad", 2, 1, "10.0.0.2"),
			racePool("us-central-ord-1", "ord", 2, 0),
			racePool("us-central-dfw-1", "dfw", 2, 0),
		},
	}
	b := newBidder(consolidateSettings())

	acts, _ := b.Plan(t0.Add(time.Hour), sp, nodes("10.0.0.1", "10.0.0.2")) // 10.0.0.3 still booting
	want := []string{
		"update-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 1",
		"delete-pool spot-bidder-us-central-ord-1 ch.vs1.large-ord 0",
		"delete-pool spot-bidder-us-central-dfw-1 ch.vs1.large-dfw 0",
		"delete-cloudspace spot-bidder-us-central-ord-1  0",
		"delete-cloudspace spot-bidder-us-central-dfw-1  0",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("while home boots:\n got %q\nwant %q", got, want)
	}

	sp.Pools = sp.Pools[:2]
	sp.Pools[1].Desired = 1
	sp.Cloudspaces = sp.Cloudspaces[:2]
	acts, _ = b.Plan(t0.Add(time.Hour+5*time.Minute), sp, nodes("10.0.0.1", "10.0.0.2", "10.0.0.3"))
	want = []string{
		"delete-pool spot-bidder-us-east-iad-1 ch.vs1.large-iad 0",
		"delete-cloudspace spot-bidder-us-east-iad-1  0",
	}
	if got := kinds(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("once home is ready:\n got %q\nwant %q", got, want)
	}

	// Settled: one region, nothing else to do.
	sp.Pools = sp.Pools[:1]
	sp.Cloudspaces = sp.Cloudspaces[:1]
	if acts, _ := b.Plan(t0.Add(time.Hour+10*time.Minute), sp, nodes("10.0.0.1", "10.0.0.3")); len(acts) != 0 {
		t.Fatalf("settled: unexpected actions %q", kinds(acts))
	}
}
