# rackspace-spot-multi-cloudspace-k8s-bidder

A small Kubernetes controller that keeps enough **Rackspace Spot VMs**
([VM Cloudspaces][spot-vm-docs]) bid for and assigned so that pods
tolerating the spot taint never stay `Pending`. It spreads bids over
**several regions**, one VM Cloudspace per region, and moves to another
region when one does not deliver.

It is the companion of
[rackspace-spot-vm-cloudspace-k8s-cleaner][cleaner], which removes the k3s
nodes of VMs that disappeared. Run both.

## Why

With a single cloudspace and a fixed $0.01 bid, Spot often answers "No
Winning Bids". You then wait about 20 minutes to find out, pick another
region by hand, and wait again. The bidder automates that loop and adds VMs
when spot workloads do not fit.

## How it works

Every `INTERVAL` (default `60s`) the bidder:

1. **Reads Spot.** It reads the server class catalog (market and minimum
   prices) and every VM Cloudspace with its pools: desired count, won
   count, and assigned servers with their IPs.
2. **Reads the cluster.**
   - **Spot nodes** match `SPOT_NODE_SELECTOR`. For each node the bidder
     reads its Ready state, IPs, allocatable resources and the requests of
     the pods on it.
   - **Pending spot pods** are pods that meet all of these:
     - `Unschedulable` for at least `PENDING_GRACE`;
     - tolerate `SPOT_TAINT`;
     - their nodeSelector/required node affinity matches a node labelled
       `SPOT_NODE_LABELS`;
     - not DaemonSet pods;
     - not stuck on a volume pinned to another node.
3. **Computes what is needed beyond the won VMs**, as the larger of:
   - **baseline**: enough VMs to reach `MIN_MEMORY` of nominal Spot RAM
     (16Gi = 2 × `ch.vs1.large`);
   - **pending demand**: pending pods are first-fit packed into the free
     room of Ready spot nodes, then into new VM-sized bins
     (`ALLOCATABLE_MEMORY_RATIO` × RAM minus DaemonSet overhead). Each new
     bin is one VM. Won VMs whose node has not joined yet (`BOOT_GRACE`)
     are subtracted.

   The total of won and requested VMs is capped by `MAX_VMS`.
4. **Bids, cancels or releases:**
   - **Short:** it adds VMs one at a time to the best *offering*: a server
     class from `SERVER_CLASSES` in a `REGIONS` region whose market price
     and minimum bid are ≤ `BID_PRICE`, and which is not cooling down.
     Offerings are ranked by lowest market price, then region order, then
     class order. An offering that already has an unfilled bid ranks last,
     so a shortfall of 2 VMs goes to 2 regions at once. Whichever fills
     first wins, and the other bid is withdrawn once capacity is met. It
     reuses `spot-bidder-<region>` cloudspaces and creates them as needed.
   - **Unfilled bid times out:** if a pool still has fewer servers than
     desired after `PROVISION_TIMEOUT` (default `30m`), the bidder shrinks
     it to what it won, or deletes it if it won nothing. That region/class
     is skipped for `REGION_COOLDOWN` (default `2h`) and the next tick bids
     elsewhere. The same applies when Spot reclaims VMs (outbid) and does
     not give them back.
   - **Surplus:** with nothing pending and nothing in flight, if spot node
     requests would fit in `SCALE_DOWN_UTILIZATION` of the nodes left after
     removing one VM, for `SCALE_DOWN_DELAY`, it releases one VM (most
     expensive market first). It never goes below `MIN_MEMORY`.
   - It keeps managed pools on `BID_PRICE` and deletes managed cloudspaces
     left without pools.

**Pricing.** Spot bills the *market price, up to your bid*, so the bidder
always bids exactly `BID_PRICE` and simply avoids offerings whose market is
above it.

**Ownership.** The bidder only ever modifies cloudspaces whose name starts
with `NAME_PREFIX` (`spot-bidder-`). With `COUNT_UNMANAGED=true` (default),
servers won by your manual cloudspaces count as capacity but are never
touched. Their unfilled bids do *not* count, so delete manual cloudspaces
you no longer want; a manual pool stuck on "No Winning Bids" can otherwise
fill later and add a surplus the bidder cannot remove.

**State.** Timers (pending bids, region cooldowns, scale-down delay) live in
memory. A restart starts them over, which errs on the side of waiting
longer.

**Errors.** A failed tick is logged and re-planned from fresh state on the
next one; `MAX_CONSECUTIVE_FAILURES` (default 5) in a row exits non-zero.

## Node requirements

VMs boot with the script from the `spot-bidder` ConfigMap (Spot's
`vmUserData`). Each node must:

- carry the taint `spot=true:NoSchedule` (`SPOT_TAINT`);
- carry the label `capacity-type=spot` (`SPOT_NODE_SELECTOR`);
- report the Spot VM's IP as an `InternalIP` or `ExternalIP` (used to tell
  Ready VMs from booting ones).

The [current script][gist] already does all three.

## Configuration reference

| Env var | Default | Description |
|---|---|---|
| `SPOT_REFRESH_TOKEN` | — (required) | Rackspace Spot refresh token. |
| `SPOT_ORG` | — (required) | Spot organization name. |
| `REGIONS` | `uk-lon-1,us-east-iad-1,us-central-ord-1,us-central-dfw-1` | Allowed regions; order breaks price ties. |
| `SERVER_CLASSES` | `ch.vs1.large` | Server class families (name without region suffix), preferred first. |
| `BID_PRICE` | `0.01` | Fixed max bid, $/hour/VM. |
| `MIN_MEMORY` | `16Gi` | Nominal Spot RAM always kept won. |
| `MAX_VMS` | `6` | Cap on won + in-flight VMs. |
| `MAX_VMS_PER_REGION` | `0` | Per-region cap, 0 = unlimited. |
| `PROVISION_TIMEOUT` | `30m` | Unfilled bid → withdraw and fail over. |
| `REGION_COOLDOWN` | `2h` | How long a failed region/class is skipped. |
| `BOOT_GRACE` | `20m` | A won VM not yet Ready still absorbs pending pods for this long. |
| `PENDING_GRACE` | `60s` | A pod must be Unschedulable this long to count. |
| `ALLOCATABLE_MEMORY_RATIO` | `0.9` | Node allocatable / nominal RAM (an 8GB VM shows ~7.26Gi). |
| `SCALE_DOWN` | `true` | Release surplus VMs above `MIN_MEMORY`. |
| `SCALE_DOWN_DELAY` | `30m` | Underuse duration before releasing one VM. |
| `SCALE_DOWN_UTILIZATION` | `0.6` | Max request/allocatable ratio on the remaining nodes. |
| `SPOT_TAINT` | `spot=true:NoSchedule` | Taint carried by spot nodes. |
| `SPOT_NODE_SELECTOR` | `capacity-type=spot` | Label selector of spot nodes. |
| `SPOT_NODE_LABELS` | `capacity-type=spot,kubernetes.io/arch=amd64,kubernetes.io/os=linux` | Labels a new spot node will have (matched against pod affinity). |
| `NAME_PREFIX` | `spot-bidder-` | Prefix of managed cloudspaces. |
| `SSH_KEY_NAME` | `default` | Spot VM SSH key for new cloudspaces. |
| `VM_IMAGE` | `ubuntu24.04` | Image for new pools. |
| `USER_DATA_FILE` | `/etc/spot-bidder/user-data` | Startup script for new pools. |
| `COUNT_UNMANAGED` | `true` | Count won servers of manual cloudspaces. |
| `INTERVAL` | `60s` | Reconcile period. |
| `MAX_CONSECUTIVE_FAILURES` | `5` | Failed ticks in a row before exiting. |
| `DRY_RUN` | `false` | Log the Spot actions without applying them. |
| `LOG_LEVEL` | — | `debug` logs every pending pod and skipped offering. |

## Deployment

1. The GitHub workflow builds and publishes
   `ghcr.io/unixfox/rackspace-spot-multi-cloudspace-k8s-bidder` (amd64 +
   arm64) on every push; `latest` tracks the default branch.
2. Create the token Secret:

   ```bash
   kubectl -n kube-system create secret generic spot-bidder \
     --from-file=SPOT_REFRESH_TOKEN=token
   ```

3. Review the ConfigMap script and the env in `deploy/deployment.yaml`,
   then apply:

   ```bash
   kubectl apply -f deploy/serviceaccount.yaml \
                 -f deploy/clusterrole.yaml \
                 -f deploy/clusterrolebinding.yaml \
                 -f deploy/configmap.yaml \
                 -f deploy/deployment.yaml
   ```

4. The manifest starts with `DRY_RUN=true`. Watch the planned actions:

   ```bash
   kubectl -n kube-system logs -f deploy/spot-bidder
   ```

   Then set `DRY_RUN=false`.

RBAC is read-only on `nodes` and `pods`; all writes go to the Spot API.

### Running locally

With `KUBECONFIG` set, the bidder uses it instead of the in-cluster config:

```bash
SPOT_REFRESH_TOKEN=$(cat token) SPOT_ORG=unixfox DRY_RUN=true \
  USER_DATA_FILE=./user-data KUBECONFIG=~/.kube/config go run .
```

## Local build & test

```bash
go build ./...
go vet ./...
gofmt -l .
go test ./...
```

[spot-vm-docs]: https://spot.rackspace.com/docs/en/virtual-machines
[cleaner]: https://github.com/unixfox/rackspace-spot-vm-cloudspace-k8s-cleaner
[gist]: https://gist.github.com/unixfox/7e93f23e0d6eba9252354271aa49cda9
