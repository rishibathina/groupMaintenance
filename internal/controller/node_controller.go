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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	outOfServiceKey = "node.kubernetes.io/out-of-service:NoExecute"
	outOfServiceEffect = v1.TaintEffectNoExecute
)

// NodeReconciler reconciles a Node object
type NodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	ProjectNumber        int
	ClusterName          string
	Location             string
	nodeStatusCache map[string]v1.NodeStatus
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
	_ = log.FromContext(ctx)
	log.Info("Reconciling on node")

	n := &v1.node{}
	err := r.Client.Get(ctx, req.NamespacedName, n)
	if err != nil {
		log.Error(err, "Error getting Node", nodeName, req.NamespacedName)
		return ctrl.Result{}, nil
	}

	if hasTaintKey(n, outOfServiceKey) { // has out-of-service

		// get the taint applciaiton time
		taintApplyTime, err := getStartTime(n, outOfServiceKey)
		if err != nil { // if fail remove regardless
			log.Error(err, "Failed to get taint start time")

			err := removeGroupTaintNp(n, outOfServiceKey)
			if err != nil {
				log.Error(err, "Failed to remove taints")
				requeue for 15 seconds at the end
			}
		}


		if the time has been at least 1 minute and 50 seconds since applicaiton {
			remove the taint 
		}
	}


	// TODO(user): your logic here
	for _, c := range cs {
		if (c.Status == v1.ConditionUnknown || c.Status == v1.ConditionFalse) { // need to operate on node
			if !hasTaintKey(n, outOfServiceKey) {
				instanceName, err := getVMInstance(n)
				if err != nil {
					log.Error(err, "failed to marshal patch for taints update", "nodeName", nodeName, "taint", nodeTermTaint)
					break
				}
				
				vmRepairing, err := checkVMRepairing(instanceName)

				if vmRepairing{
					log.Info("VM for ", instanceName, " is in REPAIRING")

					err := patchGroupTaint(n)
					if err != nil {
						log.Error(err, "group taint was not applied")
						break
					}

					log.Info("Taint successfully applied")

					// TODO: Requeue for two minutes 
					return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
				} else {
					log.Info("VM for ", instanceName, " is not in REPAIRING")
				}
			} else {
				log.Info("Found an unhealthy nodes that already has the groupTaint")
			}
			break
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Complete(r)
}

// Check if Taint with the given key exists on the node
func hasTaintKey(node *v1.Node, searchTaintKey string) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == searchTaintKey {
			return true
		}
	}
	return false
}

// Get the VM instance of the node
func getVMInstance(node *v1.Node) string, error {
	annotations := node.Annotations
	nodeIDAnnotationValue, ok := annotations["csi.volume.kubernetes.io/nodeid"]

	if ok {
		log.Info("Found csi.volume.kubernetes.io/nodeid annotation on node ", node.Name, ": ", nodeIDAnnotationValue)
	} else {
		log.Info("csi.volume.kubernetes.io/nodeid annotation not found on node ", node.Name)
	}
	var nodeid map[string]string

	err := json.Unmarshal([]byte(annotationValue), &nodeid)
	if err != nil {
		return "", log.Error(err, "failed to unmarshal annotation JSON: ")
	}

	if pdCSIValue, ok := nodeid["pd.csi.storage.gke.io"]; ok {
		parts := strings.Split(pdCSIValue, "/")
		if len(parts) > 0 && parts[len(parts)-2] == "instances" {
			return parts[len(parts)-1], nil
		}
		return "", log.Error("instance name not found in expected format: ", pdCSIValue)
	}

	return "", log.Error("key 'pd.csi.storage.gke.io' not found in annotation")
}

// Check if the VM instance is in repairing status
func checkVMRepairing(instanceName string) bool, error {

}

// Patches the node with the group taint 
func patchGroupTaint(n *v1.Node) error {
	currentTime := time.Now() 
	startTime := currentTime.Format(time.RFC3339)

	groupTaint := &v1.Taint{
		Key: outOfServiceKey,
		Value: startTime,
		Effect: outOfServiceEffect,
	}

	updatedTaints := append(n.Spec.Taints, *groupTaint)
	patch := map[string][]v1.Taint{
		"spec": {
			updatedTaints,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Error(err, "failed to marshal patch for taints update", "nodeName", nodeName, "taint", nodeTermTaint)
		return err
	}

	err = r.Client.Patch(ctx, n, client.RawPatch(types.MergePatchType, patchBytes))
	if err != nil {
		log.Error(err, "error applying taint on node using append and patch", "nodeName", nodeName, "taint", nodeTermTaint)
		return err
	}

	return nil
}

func taintApplyTime(n *v1.Node, taintKey string) string, error {

}

func removeGroupTaintNp(n *v1.Node, taintKey string) string, error {
	
}