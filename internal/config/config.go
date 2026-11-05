package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
)

// Config holds all runtime configuration for the bidder, loaded from
// environment variables (with sensible defaults where it makes sense).
type Config struct {
	// Spot platform credentials & endpoint.
	SpotRefreshToken string `envconfig:"SPOT_REFRESH_TOKEN" required:"true"`
	SpotOrg          string `envconfig:"SPOT_ORG" required:"true"`
	SpotBaseURL      string `envconfig:"SPOT_BASE_URL" default:"https://spot.rackspace.com"`
	SpotAuthURL      string `envconfig:"SPOT_AUTH_URL" default:"https://login.spot.rackspace.com"`

	// Where the bidder may place bids, in order of preference (used to break
	// ties between equally priced offerings). Defaults to the
	// "Cost-Optimized" (generation v1) regions minus Sydney and Hong Kong.
	Regions []string `envconfig:"REGIONS" default:"uk-lon-1,us-east-iad-1,us-central-ord-1,us-central-dfw-1"`
	// Server class families to bid on, in order of preference. A family is a
	// class name without its region suffix: "ch.vs1.large" matches
	// "ch.vs1.large-ord", "ch.vs1.large-lon", ...
	ServerClasses []string `envconfig:"SERVER_CLASSES" default:"ch.vs1.large"`
	// Fixed max bid, $/hour per VM. You pay the market price, up to this.
	BidPrice string `envconfig:"BID_PRICE" default:"0.01"`

	// Sizing.
	MinMemoryRaw string `envconfig:"MIN_MEMORY" default:"16Gi"` // nominal Spot RAM to always keep won
	MinMemory    int64  `ignored:"true"`
	MaxVMs       int    `envconfig:"MAX_VMS" default:"6"`            // hard cap on won + in-flight VMs
	MaxPerRegion int    `envconfig:"MAX_VMS_PER_REGION" default:"0"` // 0 = unlimited
	// Fraction of a VM's nominal RAM that ends up allocatable on the node
	// (an 8GB ch.vs1.large reports ~7.26Gi allocatable).
	AllocatableMemoryRatio float64 `envconfig:"ALLOCATABLE_MEMORY_RATIO" default:"0.9"`

	// Region failover.
	ProvisionTimeout time.Duration `envconfig:"PROVISION_TIMEOUT" default:"30m"` // unfilled bid → give up on that region
	RegionCooldown   time.Duration `envconfig:"REGION_COOLDOWN" default:"2h"`    // how long a failed region is skipped
	BootGrace        time.Duration `envconfig:"BOOT_GRACE" default:"20m"`        // won VM not yet a Ready node still counts for pending pods

	// Scale-down of surplus VMs (above MIN_MEMORY) once spot nodes are idle.
	ScaleDown            bool          `envconfig:"SCALE_DOWN" default:"true"`
	ScaleDownDelay       time.Duration `envconfig:"SCALE_DOWN_DELAY" default:"30m"`
	ScaleDownUtilization float64       `envconfig:"SCALE_DOWN_UTILIZATION" default:"0.6"`

	// What a spot pod / spot node looks like in the cluster.
	SpotTaintRaw      string            `envconfig:"SPOT_TAINT" default:"spot=true:NoSchedule"`
	SpotTaint         corev1.Taint      `ignored:"true"`
	SpotNodeSelector  string            `envconfig:"SPOT_NODE_SELECTOR" default:"capacity-type=spot"`
	SpotNodeLabelsRaw string            `envconfig:"SPOT_NODE_LABELS" default:"capacity-type=spot,kubernetes.io/arch=amd64,kubernetes.io/os=linux"`
	SpotNodeLabels    map[string]string `ignored:"true"`
	PendingGrace      time.Duration     `envconfig:"PENDING_GRACE" default:"60s"`

	// What the bidder creates.
	NamePrefix   string `envconfig:"NAME_PREFIX" default:"spot-bidder-"`
	SSHKeyName   string `envconfig:"SSH_KEY_NAME" default:"default"`
	VMImage      string `envconfig:"VM_IMAGE" default:"ubuntu24.04"`
	UserDataFile string `envconfig:"USER_DATA_FILE" default:"/etc/spot-bidder/user-data"`
	UserData     string `ignored:"true"`

	// Whether won VMs of cloudspaces not created by the bidder count towards
	// capacity (they are never modified).
	CountUnmanaged bool `envconfig:"COUNT_UNMANAGED" default:"true"`

	Interval               time.Duration `envconfig:"INTERVAL" default:"60s"`
	MaxConsecutiveFailures int           `envconfig:"MAX_CONSECUTIVE_FAILURES" default:"5"`
	DryRun                 bool          `envconfig:"DRY_RUN" default:"false"`
}

// Load reads configuration from environment variables and validates it.
func Load() (Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return c, fmt.Errorf("envconfig: %w", err)
	}

	if strings.TrimSpace(c.SpotRefreshToken) == "" {
		return c, fmt.Errorf("SPOT_REFRESH_TOKEN is required")
	}
	if strings.TrimSpace(c.SpotOrg) == "" {
		return c, fmt.Errorf("SPOT_ORG is required")
	}
	c.Regions = trimAll(c.Regions)
	c.ServerClasses = trimAll(c.ServerClasses)
	if len(c.Regions) == 0 || len(c.ServerClasses) == 0 {
		return c, fmt.Errorf("REGIONS and SERVER_CLASSES must not be empty")
	}
	if bid, err := strconv.ParseFloat(c.BidPrice, 64); err != nil || bid <= 0 {
		return c, fmt.Errorf("BID_PRICE must be a positive number, got %q", c.BidPrice)
	}

	q, err := resource.ParseQuantity(c.MinMemoryRaw)
	if err != nil {
		return c, fmt.Errorf("MIN_MEMORY: %w", err)
	}
	c.MinMemory = q.Value()

	if c.MaxVMs < 1 {
		return c, fmt.Errorf("MAX_VMS must be >= 1, got %d", c.MaxVMs)
	}
	if c.AllocatableMemoryRatio <= 0 || c.AllocatableMemoryRatio > 1 {
		return c, fmt.Errorf("ALLOCATABLE_MEMORY_RATIO must be in (0,1], got %v", c.AllocatableMemoryRatio)
	}
	if c.Interval <= 0 || c.ProvisionTimeout <= 0 {
		return c, fmt.Errorf("INTERVAL and PROVISION_TIMEOUT must be > 0")
	}

	c.SpotTaint, err = parseTaint(c.SpotTaintRaw)
	if err != nil {
		return c, err
	}
	if _, err := labels.Parse(c.SpotNodeSelector); err != nil {
		return c, fmt.Errorf("SPOT_NODE_SELECTOR: %w", err)
	}
	set, err := labels.ConvertSelectorToLabelsMap(c.SpotNodeLabelsRaw)
	if err != nil {
		return c, fmt.Errorf("SPOT_NODE_LABELS: %w", err)
	}
	c.SpotNodeLabels = set

	data, err := os.ReadFile(c.UserDataFile)
	if err != nil {
		return c, fmt.Errorf("USER_DATA_FILE: %w", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return c, fmt.Errorf("USER_DATA_FILE %q is empty", c.UserDataFile)
	}
	c.UserData = string(data)

	return c, nil
}

// parseTaint parses "key=value:Effect" (or "key:Effect").
func parseTaint(s string) (corev1.Taint, error) {
	kv, effect, ok := strings.Cut(s, ":")
	if !ok || effect == "" {
		return corev1.Taint{}, fmt.Errorf("SPOT_TAINT must look like key=value:Effect, got %q", s)
	}
	key, value, _ := strings.Cut(kv, "=")
	return corev1.Taint{Key: key, Value: value, Effect: corev1.TaintEffect(effect)}, nil
}

func trimAll(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
