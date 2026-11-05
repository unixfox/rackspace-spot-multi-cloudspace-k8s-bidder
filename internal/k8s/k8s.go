// Package k8s reads the cluster-side demand for spot capacity: pods that
// tolerate the spot taint but cannot be scheduled, and how full the existing
// spot nodes are.
package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	resourcehelper "k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"

	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/model"
)

// Client reads pods and nodes and classifies them against the spot node
// template.
type Client struct {
	Clientset kubernetes.Interface

	SpotTaint        corev1.Taint      // taint carried by every spot node
	SpotNodeSelector string            // label selector matching spot nodes
	SpotNodeLabels   map[string]string // labels a new spot node will have (for nodeSelector/affinity checks)
	PendingGrace     time.Duration     // a pod must be Unschedulable for this long to count
}

// NewInCluster builds a Clientset from in-cluster config and a
// ServiceAccount. It fails fast if not running inside a cluster, unless
// KUBECONFIG is set (for local DRY_RUN testing).
func NewInCluster() (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error
	if path := os.Getenv("KUBECONFIG"); path != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", path)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("k8s: load in-cluster config (are we running in a pod? set KUBECONFIG to run locally): %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s: build clientset: %w", err)
	}
	return cs, nil
}

// State lists spot nodes and all pods, and returns the pending spot demand
// plus per-spot-node usage.
func (c *Client) State(ctx context.Context, now time.Time) (model.ClusterState, error) {
	nodes, err := c.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: c.SpotNodeSelector})
	if err != nil {
		return model.ClusterState{}, fmt.Errorf("k8s: list nodes with selector %q: %w", c.SpotNodeSelector, err)
	}
	pods, err := c.Clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return model.ClusterState{}, fmt.Errorf("k8s: list pods: %w", err)
	}

	st := model.ClusterState{}
	byName := make(map[string]int, len(nodes.Items))
	for i := range nodes.Items {
		n := &nodes.Items[i]
		byName[n.Name] = len(st.SpotNodes)
		st.SpotNodes = append(st.SpotNodes, model.SpotNode{
			Name:        n.Name,
			Ready:       isReady(n),
			IPs:         addresses(n),
			Allocatable: toResources(n.Status.Allocatable),
		})
	}

	template := c.templateNode()
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		req := toResources(resourcehelper.PodRequests(p, resourcehelper.PodResourcesOptions{}))

		if idx, ok := byName[p.Spec.NodeName]; ok && p.Spec.NodeName != "" {
			n := &st.SpotNodes[idx]
			n.Requested = n.Requested.Add(req)
			if isDaemonSetPod(p) {
				n.DaemonSet = n.DaemonSet.Add(req)
			}
			continue
		}

		if ok, why := c.wantsSpotCapacity(p, template, now); ok {
			slog.Debug("pending spot pod", "pod", p.Namespace+"/"+p.Name,
				"cpu_m", req.MilliCPU, "memory", req.Memory)
			st.Pending = append(st.Pending, req)
		} else if why != "" {
			slog.Debug("ignoring unschedulable pod", "pod", p.Namespace+"/"+p.Name, "reason", why)
		}
	}
	return st, nil
}

// wantsSpotCapacity reports whether p is an Unschedulable pod that a new
// spot node could host. The second value explains why an Unschedulable pod
// was ignored (empty for pods that are simply not pending).
func (c *Client) wantsSpotCapacity(p *corev1.Pod, template *corev1.Node, now time.Time) (bool, string) {
	if p.Spec.NodeName != "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodPending {
		return false, ""
	}
	var cond *corev1.PodCondition
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == corev1.PodScheduled {
			cond = &p.Status.Conditions[i]
		}
	}
	if cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != corev1.PodReasonUnschedulable {
		return false, ""
	}
	if now.Sub(cond.LastTransitionTime.Time) < c.PendingGrace {
		return false, "unschedulable for less than PENDING_GRACE"
	}
	if isDaemonSetPod(p) {
		return false, "daemonset pod"
	}
	if !corev1helpers.TolerationsTolerateTaint(klog.Background(), p.Spec.Tolerations, &c.SpotTaint, false) {
		return false, "does not tolerate the spot taint"
	}
	if ok, err := nodeaffinity.GetRequiredNodeAffinity(p).Match(template); err != nil || !ok {
		return false, "nodeSelector/affinity does not match a spot node"
	}
	if strings.Contains(cond.Message, "volume node affinity conflict") {
		return false, "bound to a volume pinned to another node"
	}
	return true, ""
}

func (c *Client) templateNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "spot-template", Labels: c.SpotNodeLabels},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{c.SpotTaint}},
	}
}

func isReady(n *corev1.Node) bool {
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func addresses(n *corev1.Node) []string {
	var out []string
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP || a.Type == corev1.NodeExternalIP {
			out = append(out, a.Address)
		}
	}
	return out
}

func isDaemonSetPod(p *corev1.Pod) bool {
	for _, o := range p.OwnerReferences {
		if o.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func toResources(l corev1.ResourceList) model.Resources {
	return model.Resources{
		MilliCPU: l.Cpu().MilliValue(),
		Memory:   l.Memory().Value(),
	}
}
