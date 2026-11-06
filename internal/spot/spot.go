// Package spot wraps the Rackspace Spot SDK: it reads server classes, VM
// Cloudspaces and VM pools into model types, and applies the bidder's
// mutations (create cloudspace/pool, resize pool, delete pool/cloudspace).
package spot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	v1 "github.com/rackspace-spot/spot-go-sdk/api/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/model"
)

// API is the subset of the Spot SDK client the bidder uses. Declared as an
// interface so tests can substitute a fake.
type API interface {
	Authenticate(ctx context.Context) (string, error)
	ListServerClasses(ctx context.Context, region string) (*v1.ServerClassList, error)
	ListVMCloudSpaces(ctx context.Context, org string) (*v1.VMCloudSpaceList, error)
	CreateVMCloudSpace(ctx context.Context, vmcs v1.VMCloudSpace) error
	DeleteVMCloudSpace(ctx context.Context, org, name string) error
	GetOrgID(ctx context.Context, orgNameOrID string) (bool, string, error)
	UpdateVMPool(ctx context.Context, org string, pool v1.VMPool) error
	DeleteVMPool(ctx context.Context, org, name string) error
}

// Client wraps a Spot SDK client, the org to manage, and what new pools look
// like.
type Client struct {
	Spot       API
	HTTP       *http.Client // for calls the SDK gets wrong (see CreatePool)
	BaseURL    string
	Org        string
	NamePrefix string // cloudspaces whose name starts with this are managed
	SSHKeyName string
	VMImage    string
	UserData   string // raw script, base64-encoded on pool creation
}

// State reads all server classes and VM Cloudspaces (with their pools).
func (c *Client) State(ctx context.Context) (model.SpotState, error) {
	// The SDK caches the id token and only refreshes it once expired; it
	// does not refresh on its own before each call.
	if _, err := c.Spot.Authenticate(ctx); err != nil {
		return model.SpotState{}, fmt.Errorf("spot: authenticate: %w", err)
	}

	scs, err := c.Spot.ListServerClasses(ctx, "")
	if err != nil {
		return model.SpotState{}, fmt.Errorf("spot: list server classes: %w", err)
	}
	st := model.SpotState{ServerClasses: make(map[string]model.ServerClass, len(scs.Items))}
	for _, sc := range scs.Items {
		cls, err := toServerClass(sc)
		if err != nil {
			return model.SpotState{}, fmt.Errorf("spot: server class %q: %w", sc.Name, err)
		}
		st.ServerClasses[cls.Name] = cls
	}

	list, err := c.Spot.ListVMCloudSpaces(ctx, c.Org)
	if err != nil {
		return model.SpotState{}, fmt.Errorf("spot: list VM cloudspaces for org %q: %w", c.Org, err)
	}
	if list == nil {
		return st, nil
	}
	for _, vmcs := range list.Items {
		managed := strings.HasPrefix(vmcs.Name, c.NamePrefix)
		st.Cloudspaces = append(st.Cloudspaces, model.Cloudspace{
			Name:    vmcs.Name,
			Region:  vmcs.Region,
			Managed: managed,
			Created: vmcs.CreationTimestamp,
		})

		// A pool's wonCount (and bidStatus "Fulfilled") only means the
		// auction was won: Spot can then sit in "Provisioning/Reserving" for
		// hours without allocating a server when the region has no physical
		// capacity. Only servers actually assigned to the cloudspace count.
		ips := map[string][]string{}
		servers := map[string]int{}
		for _, srv := range vmcs.AssignedServers {
			if srv.NodePoolName == "" {
				continue
			}
			servers[srv.NodePoolName]++
			if srv.IPAddress != "" {
				ips[srv.NodePoolName] = append(ips[srv.NodePoolName], srv.IPAddress)
			}
		}
		for _, p := range vmcs.VMPools {
			if p == nil {
				continue
			}
			st.Pools = append(st.Pools, model.Pool{
				Name:        p.Name,
				Cloudspace:  vmcs.Name,
				Region:      vmcs.Region,
				ServerClass: p.ServerClass,
				Managed:     managed,
				Desired:     p.Desired,
				Won:         servers[p.Name],
				WonBids:     p.WonCount,
				BidPrice:    p.BidPrice,
				BidStatus:   p.BidStatus,
				IPs:         ips[p.Name],
			})
		}
	}
	return st, nil
}

// CreateCloudspace creates a VM Cloudspace in region.
func (c *Client) CreateCloudspace(ctx context.Context, name, region string) error {
	err := c.Spot.CreateVMCloudSpace(ctx, v1.VMCloudSpace{
		Name:        name,
		Org:         c.Org,
		Region:      region,
		VMSshKeyRef: v1.VMSshKeyRef{Name: c.SSHKeyName},
	})
	if err != nil {
		return fmt.Errorf("spot: create VM cloudspace %q in %q: %w", name, region, err)
	}
	return nil
}

// CreatePool creates a spot VM pool in an existing cloudspace.
//
// This does not use the SDK's CreateVMPool: as of spot-go-sdk v0.2.1 it
// omits spec.vmCloudSpaceRef, which the API now requires (HTTP 422
// "spec.vmCloudSpaceRef in body must be of type object"). The body below
// mirrors what the Spot console sends.
func (c *Client) CreatePool(ctx context.Context, name, cloudspace, serverClass string, desired int, bid string) error {
	wrap := func(err error) error {
		return fmt.Errorf("spot: create VM pool %q (%s, desired %d) in %q: %w", name, serverClass, desired, cloudspace, err)
	}
	token, err := c.Spot.Authenticate(ctx)
	if err != nil {
		return wrap(fmt.Errorf("authenticate: %w", err))
	}
	exists, ns, err := c.Spot.GetOrgID(ctx, c.Org)
	if err != nil {
		return wrap(fmt.Errorf("resolve org %q: %w", c.Org, err))
	}
	if !exists {
		return wrap(fmt.Errorf("organization %q not found", c.Org))
	}

	ref := map[string]string{"name": cloudspace, "namespace": ns}
	body, err := json.Marshal(map[string]any{
		"apiVersion": "ngpc.rxt.io/v1",
		"kind":       "VMPool",
		"metadata":   map[string]string{"name": name, "namespace": ns},
		"spec": map[string]any{
			"bidPrice":        bid,
			"desired":         desired,
			"poolType":        "spot",
			"serverClass":     serverClass,
			"vmCloudSpace":    cloudspace,
			"vmCloudSpaceRef": ref,
			"vmImage":         c.VMImage,
			"vmUserData":      v1.PrepareUserData(c.UserData),
		},
	})
	if err != nil {
		return wrap(err)
	}

	url := fmt.Sprintf("%s/apis/ngpc.rxt.io/v1/namespaces/%s/vmpools", c.BaseURL, ns)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return wrap(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return wrap(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return wrap(&v1.HTTPStatusError{StatusCode: resp.StatusCode, Body: string(msg)})
	}
	return nil
}

// UpdatePool sets a pool's desired count and bid. desired must be >= 1: the
// update body omits a zero desired, so shrinking to zero is DeletePool.
func (c *Client) UpdatePool(ctx context.Context, name string, desired int, bid string) error {
	if desired < 1 {
		return fmt.Errorf("spot: update VM pool %q: desired must be >= 1, got %d", name, desired)
	}
	if err := c.Spot.UpdateVMPool(ctx, c.Org, v1.VMPool{Name: name, Desired: desired, BidPrice: bid}); err != nil {
		return fmt.Errorf("spot: update VM pool %q (desired %d, bid %s): %w", name, desired, bid, err)
	}
	return nil
}

// DeletePool deletes a VM pool, releasing all its servers. Already gone is
// success.
func (c *Client) DeletePool(ctx context.Context, name string) error {
	if err := c.Spot.DeleteVMPool(ctx, c.Org, name); err != nil && !isNotFound(err) {
		return fmt.Errorf("spot: delete VM pool %q: %w", name, err)
	}
	return nil
}

// DeleteCloudspace deletes a VM Cloudspace. Already gone is success.
func (c *Client) DeleteCloudspace(ctx context.Context, name string) error {
	if err := c.Spot.DeleteVMCloudSpace(ctx, c.Org, name); err != nil && !isNotFound(err) {
		return fmt.Errorf("spot: delete VM cloudspace %q: %w", name, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var se *v1.HTTPStatusError
	if errors.As(err, &se) {
		return se.StatusCode == 404
	}
	return strings.Contains(err.Error(), "not found")
}

func toServerClass(sc v1.ServerClass) (model.ServerClass, error) {
	cpu, err := resource.ParseQuantity(sc.Resources.CPU)
	if err != nil {
		return model.ServerClass{}, fmt.Errorf("cpu %q: %w", sc.Resources.CPU, err)
	}
	mem, err := parseMemory(sc.Resources.Memory)
	if err != nil {
		return model.ServerClass{}, err
	}
	market, err := parsePrice(sc.CurrentMarketPricePerHour)
	if err != nil {
		return model.ServerClass{}, fmt.Errorf("market price: %w", err)
	}
	minBid, err := parsePrice(sc.MinBidPricePerHour)
	if err != nil {
		return model.ServerClass{}, fmt.Errorf("min bid price: %w", err)
	}
	return model.ServerClass{
		Name:        sc.Name,
		Region:      sc.Region,
		Resources:   model.Resources{MilliCPU: cpu.MilliValue(), Memory: mem},
		MarketPrice: market,
		MinBid:      minBid,
	}, nil
}

// parseMemory parses Spot's "8GB" style sizes. Spot's GB are binary: an
// "8GB" VM reports ~7.26Gi allocatable, consistent with 8Gi minus overhead.
func parseMemory(s string) (int64, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "i") + "i"
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, fmt.Errorf("memory %q: %w", s, err)
	}
	return q.Value(), nil
}

// parsePrice parses "$0.010000" / "0.01".
func parsePrice(s string) (float64, error) {
	return strconv.ParseFloat(strings.TrimPrefix(strings.TrimSpace(s), "$"), 64)
}
