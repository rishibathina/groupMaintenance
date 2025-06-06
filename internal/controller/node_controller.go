/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	// "log"
	"strconv"
	"strings"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	nodeName = "Node_Name"
	//Annotations
	customerTriggeredMaintenanceAnnotation = "node.gke.io/maintenance-state"
	reportAndReplaceOperationAnnotation    = "node.gke.io/report-and-replace-operation"
	//Labels
	customerTriggeredMaintenanceLabel = "cloud.google.com/perform-maintenance"
	upcomingMaintenanceLabel          = "cloud.google.com/scheduled-maintenance-time"
	latestStartTimeLabel              = "cloud.google.com/scheduled-maintenance-time-latest"
	activeMaintenanceLabel            = "cloud.google.com/active-node-maintenance"
	machineFamilyLabel                = "cloud.google.com/machine-family"
	nodePoolLabel                     = "cloud.google.com/gke-nodepool"
	faultBehaviorLabel                = "cloud.google.com/fault-behavior"
	reportAndReplaceStatusLabel       = "cloud.google.com/report-and-replace-status"
	//Taints
	maintenanceWindowStartTaintKey = "cloud.google.com/maintenance-window-started"
	nodeTerminationTaintKey        = "cloud.google.com/impending-node-termination"
	outOfServiceKey                = "node.kubernetes.io/out-of-service:NoExecute"
	outOfServiceEffect             = v1.TaintEffectNoExecute
)

var log = ctrl.Log.WithName("groupMaintenanceController")

// NodeReconciler reconciles a Node object
type NodeReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	ProjectNumber         int
	ClusterName           string
	Location              string
	triggerNodeCache      map[string]string // node that triggered group tainting -> nodepool it is a part of
	taintingNodepoolCache map[string]string // nodepool name that is tainted -> trigger node
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Node object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// _ = log.FromContext(ctx)
	log.Info("Reconciling on node")

	n := &v1.Node{}
	err := r.Client.Get(ctx, req.NamespacedName, n)
	if err != nil {
		log.Error(err, "Error getting Node", nodeName, req.NamespacedName)
		return ctrl.Result{}, nil
	}

	// REMOVING TAINT LOGIC

	// Check if the node has the taint for atleast 2 minutes
	// or the node has come back online
	requeueAtEnd := false
	removeTaint := false

	// TODO: AND IT IS IN THE CACHE
	// TODO: fix this so that it accounts for edge case where trigger node wasnt tainted but other nodes in the pool were do to sm issue when tainting
	_, ok := r.triggerNodeCache[n.Name]
	if ok { // has trigger node
		// TODO: FIX this
		if isNodeReady(n) { // if node comes back online
			removeTaint = true
		} else if hasTaintKey(n, outOfServiceKey) { // timeout case
			// get the taint application time
			// taintApplyTime == "" if apply time isnt found
			taintApplyTime := getTaintApplyTime(n, outOfServiceKey)
			if taintApplyTime == "" { // if empty remove regardless
				log.Error(err, "Failed to get taint start time")
				removeTaint = true
			} else {
				// check if taint took too long
				// if it errors in the process, returns true anyway
				taintTooLong, err := isTaintAppliedTooLong(taintApplyTime)
				if err != nil {
					log.Error(err, "Could not check if the taint has been on for too long")
					removeTaint = true
				}
				if taintTooLong {
					removeTaint = true
				}
			}
		} else { // not back online and doesnt have taint but is trigger node (issue when group patching)
			// TODO: group taint nodes
			r.addToGroupCaches(n)

			err := r.patchGroupTaint(ctx, n)
			if err != nil {
				log.Error(err, "group taint was not applied to all nodes")
				requeueAtEnd = true
			}
		}

		if removeTaint {
			err := r.removeGroupTaintNp(ctx, n, outOfServiceKey)
			if err != nil {
				log.Error(err, "Failed to remove taints")
				// requeue for 15 seconds at the end
				requeueAtEnd = true
			} else {
				// TODO: REMOVE NODE FROM CACHE AND NODPOOL FROM CACHE
				r.removeFromGroupCaches(n)
				// if err != nil {
				// 	// TODO: NOT SURE HOW TO ERROR HANDLER HERE

				// }
			}
		}
	}

	// Applying Taint logic

	// In MC this should be last
	// Check if the node needs the taint
	requeueTwoMins := false

	ok, nodepoolInCache := checkNodepoolCache(n)
	// TODO: its nodepool is not in the cache
	if isNodeNotReadyOrUnknown(n) && !removeTaint && ok && !nodepoolInCache { // need to operate on node
		// Node unready and nodepool not in cache

		if !hasTaintKey(n, outOfServiceKey) { // the node doesn't already have the taint
			hasVMInfoError := false
			projectID, zone, instanceName := getVMInfo(n)
			if projectID == "" || zone == "" || instanceName == "" {
				log.Error(err, "failed to get accurate VM info for node: ", nodeName)
				hasVMInfoError = true
			}

			vmRepairing := false
			hasRepairCheckError := false
			if !hasVMInfoError { // Only check VM repairing if VM info was successfully retrieved
				vmRepairing, err = r.checkVMRepairing(projectID, zone, instanceName, ctx)
				if err != nil {
					log.Error(err, "failed to check if VM is in REPAIRING: ", nodeName)
					hasRepairCheckError = true
				}
			}

			if vmRepairing && !hasVMInfoError && !hasRepairCheckError { // VM is in repairing and no errors occurred
				log.Info("VM for ", instanceName, " is in REPAIRING")

				// taint the nodes in the nodepool associated with this node
				// TODO: ADD TO CACHE HERE BOTH THE NODEPOOL AND THE TRIGGER NODE
				r.addToGroupCaches(n)

				err := r.patchGroupTaint(ctx, n)
				if err != nil {
					log.Error(err, "group taint was not applied to all nodes")
					requeueAtEnd = true

				} else {
					log.Info("Taint successfully applied")

					// Requeue to serve as a timeout for the taint
					requeueTwoMins = true
				}

				
			} else {
				if hasVMInfoError {
					log.Info("Skipping VM REPAIRING check due to VM info error for node: ", nodeName)
				} else if hasRepairCheckError {
					log.Info("VM for ", instanceName, " might not be in REPAIRING due to check error.")
				} else {
					log.Info("VM for ", instanceName, " is not in REPAIRING")
				}
			}
		} else { // THIS SHOULD NEVER HAPPEN SINCE IF IT HAS THE TAINT THE NODEPOOL SHOULD BE IN THE CACHE
			log.Info("Found an unhealthy nodes that already has the groupTaint")
		}

	}

	if requeueTwoMins {
		return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
	} else if requeueAtEnd { // if nothing went wrong along the way
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	} else { // if something went wrong in the process, requeue for 15 seconds later
		return ctrl.Result{}, nil
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.Node{}).
		Complete(r)
}

// TODO: ADD THIS FUNCTIONALITY
func isNodeReady(n *v1.Node) bool {
	for _, condition := range n.Status.Conditions {
		if condition.Type == v1.NodeReady {
			return condition.Status == v1.ConditionTrue
		}
	}

	return false
}

// TODO: ADD THIS FUNCTIONALITY
func isNodeNotReadyOrUnknown(n *v1.Node) bool {
	for _, condition := range n.Status.Conditions {
		if condition.Type == v1.NodeReady {
			return condition.Status == v1.ConditionUnknown || condition.Status == v1.ConditionFalse
		}
	}

	return true
}

// Check if the VM instance is in repairing status
func (r *NodeReconciler) checkVMRepairing(projectID string, zone string, instanceName string, ctx context.Context) (bool, error) {
	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		log.Error(err, "compute.NewInstancesRESTClient did not work")
		return false, err
	}
	defer instancesClient.Close()

	req := &compute.GetInstanceRequest{
		Project:  projectID,
		Zone:     zone,
		Instance: instanceName,
	}

	instance, err := instancesClient.Get(ctx, req)
	if err != nil {
		log.Error(err, "Error getting instance info")
		return false, err
	}

	if instance.GetName() != instanceName {
		log.Info("Received instance name does not match requested name, requested: ", instanceName, " received: ", instance.GetName())
		return false, nil
	}

	switch instance.GetStatus() {
	case "REPAIRING":
		log.Info("VM ", instanceName, " is currently in REPAIRING state.")
		return true, nil
	default:
		log.Info("VM ", instanceName, " is not in REPAIRING state.")
		return false, nil
	}
}

// Patches the node with the group taint
func (r *NodeReconciler) patchGroupTaint(ctx context.Context, n *v1.Node) error {
	// create groupTaint
	currentTime := time.Now()
	startTime := currentTime.Format(time.RFC3339)

	groupTaint := &v1.Taint{
		Key:    outOfServiceKey,
		Value:  startTime,
		Effect: outOfServiceEffect,
	}

	nl, err := listNodesInNodepool(&n)
	if err != nil {
		log.Error("Could not get nodes in nodepool")
		return err
	}

	// iterate through all the nodes in the pool and taint all except on the current node
	for _, otherNode := range nl.Items {
		otherNodePoolName, otherOk := otherNode.Labels[nodePoolLabel]
		if otherOk && otherNodePoolName == npName && otherNode.Name != n.Name { // checking that the nodepool is the same and it isn't the current node
			// TODO: CHECK IF THE NODE ALREADY HAS THE TAINT WE DONT WANT TO ADD TWO INSTANCES OF THE SAME TAINT
			log.Info("Patching node with group taint", "nodeName:", otherNode.Name, "taint:", groupTaint)

			// Patch the node with the group taint
			if err := r.patchTaint(ctx, &otherNode, append(otherNode.Spec.Taints, groupTaint)); err != nil {
				log.Error(err, "failed to patch node with group taint", "nodeName", otherNode.Name, "taint", groupTaint)
				return err
			}
		}
	}

	// taint the current node
	err := r.patchTaint(ctx, &n, append(n.Spec.Taints, groupTaint))
	if err != nil {
		log.Error(err, "failed to patch the current node with group taint", "nodeName", n.Name, "taint", groupTaint)
		return err
	}

	return nil
}

// Patch the node with the requested taint
func (r *NodeReconciler) patchTaint(ctx context.Context, n *v1.Node, taintsToPatch []v1.Taint) error {
	patch := map[string][]v1.Taint{
		"spec": {
			taintsToPatch,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Error(err, "failed to marshal patch for taints update", "nodeName", n.Name, "taint", taintToPatch)
		return err
	}

	err = r.Client.Patch(ctx, n, client.RawPatch(types.MergePatchType, patchBytes))
	if err != nil {
		log.Error(err, "error applying taint on node using append and patch", "nodeName", n.Name, "taint", taintToPatch)
		return err
	}

	return nil
}

// remove the taints off of the nodepool
func (r *NodeReconciler) removeGroupTaintNp(ctx context.Context, n *v1.Node, taintKey string) error {
	nl, err := listNodesInNodepool(&n)
	if err != nil {
		log.Error("Unable to list nodes in nodepool for removing taint")
		return err
	}

	for _, otherNode := range nl.Items {
		otherNodePoolName, otherOk := otherNode.Labels[nodePoolLabel]
		if otherOk && otherNodePoolName == npName && otherNode.Name != n.Name { // checking that the nodepool is the same and it isn't the current node
			log.Info("Removing group taint off of node,", "nodeName:", otherNode.Name, "taint:", taintKey)
			otherNode.Spec.Taints = removeTaintKey(taintKey, otherNode.Spec.Taints)
			if err := r.patchTaint(ctx, &otherNode, otherNode.Spec.Taints); err != nil {
				log.Error(err, "failed to remove group taint off of node", "nodeName", otherNode.Name, "taint", taintKey)
				return err
			}
		}
	}

	n.Spec.Taints = removeTaintKey(taintKey, n.Spec.Taints)
	if err := r.patchTaint(ctx, &n, n.Spec.Taints); err != nil {
		log.Error(err, "failed to remove group taint off of node", "nodeName", n.Name, "taint", taintKey)
		return err
	}
	return nil
}

// remove a specific taint from taints
// if taint does not exist, return the original list
func removeTaintKey(searchTaintKey string, taints []v1.Taint) []v1.Taint {
	index := getTaintIndex(searchTaintKey, taints)

	if index > -1 {
		return append(taints[:index], taints[index+1:]...)
	}
	return taints
}

// get the index of the taint if it exists
// otherwise return -1
func getTaintIndex(searchTaintKey string, taints []v1.Taint) int {
	index := -1
	for ind, taint := range taints {
		if taint.Key == searchTaintKey {
			index = ind
			break
		}
	}

	return index
}

// Check if Taint with the given key exists on the node
func hasTaintKey(node *v1.Node, searchTaintKey string) bool {
	taints := node.Spec.Taints

	for _, taint := range taints {
		if taint.Key == searchTaintKey {
			return true
		}
	}
	return false
}

// get the application time of the taint
func getTaintApplyTime(n *v1.Node, taintKey string) string {
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintKey {
			return taint.Value
		}
	}
	return ""
}

// Check the taint to see if it has been longer than 2 minutes since application
func isTaintAppliedTooLong(taintApplyTimeStr string) (bool, error) {
	if taintApplyTimeStr == "" {
		return true, fmt.Errorf("taint apply time string is empty")
	}

	parsedApplyTime, err := time.Parse(time.RFC3339, taintApplyTimeStr)
	if err != nil {
		return true, fmt.Errorf("error parsing taint apply time: %v", err)
	}

	currentTime := time.Now()
	durationSinceApply := currentTime.Sub(parsedApplyTime)

	return durationSinceApply >= (2 * time.Minute), nil
}

func listNodesInNodepool(n *v1.Node) (*v1.NodeList, error) {
	// Get the NodePool label
	npName, ok := n.Labels[nodePoolLabel]
	if !ok {
		return nil, fmt.Errorf("Node does not have the nodepool label", "nodeName", n.Name, "label", nodePoolLabel)
	}

	// Create the nodepool label requirement
	npLabelReq, err := labels.NewRequirement(nodePoolLabel, selection.In, []string{npName})
	if err != nil {
		log.Error("Could not create nodepool requirement")
		return nil, err
	}

	// Create selector based on the nodepool label requirement
	opts := &client.ListOptions{
		LabelSelector: labels.NewSelector().Add(*npLabelReq),
	}

	// List all nodes in the nodepool
	nl := &v1.NodeList{}
	err = r.Client.List(ctx, nl, opts)
	if err != nil {
		return nil, err
	}

	return nl, nil
}

func checkNodepoolCache(n *v1.Node) (bool, bool) {
	nodepoolInCache := false
	npName, ok := n.Labels[nodePoolLabel]
	if ok {
		_, found := r.taintingNodepoolCache[npName]
		if found {
			nodepoolInCache = true
		}
	}

	return ok, nodepoolInCache
}

func (r *NodeReconciler) removeFromGroupCaches(n *v1.Node) err {
	delete(r.triggerNodeCache, n.Name)

	npName, ok := n.Labels[nodePoolLabel]
	if !ok {
		return fmt.Errorf("Node does not have the nodepool label", "nodeName", n.Name, "label", nodePoolLabel)
	}

	delete(r.taintingNodepoolCache, npName)
	return nil
}

func (r *NodeReconciler) addToGroupCaches(n *v1.Node) err {
	npName, ok := n.Labels[nodePoolLabel]
	if !ok {
		return fmt.Errorf("Node does not have the nodepool label", "nodeName", n.Name, "label", nodePoolLabel)
	}

	if _, exists := r.triggerNodeCache[n.Name]; !exists {
		r.triggerNodeCache[n.Name] = npName
	}

	if _, exists := r.taintingNodepoolCache[npName]; !exists {
		r.taintingNodepoolCache[npName] = n.Name
	}

	return nil
}
