import (
	"context"
	"encoding/json"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	client "k8s.io/client-go/kubernetes"
)
type nodeMetadataHandler struct {
	client client.Interface
}
const (
	// FieldManager denotes the actor making the change in Kubernetes.
	FieldManager = "maintenance-controller"
)
func NewNodeMetadataHandler(client client.Interface) NodeMetadataHandler {
	return &nodeMetadataHandler{
		client: client,
	}
}
type PatchOperation func(*v1.Node, string, any) (map[string]any, bool)
func (n *nodeMetadataHandler) PatchLabelMetadata(node *v1.Node, key string, value any) (map[string]any, bool) {
	if val, present := node.GetObjectMeta().GetLabels()[key]; !present && value == nil || val == value {
		return nil, false
	}
	patch := map[string]any{ // Node
		"metadata": map[string]any{ // ObjectMeta
			"labels": map[string]any{
				key: value,
			},
		},
	}
	return patch, true
}
func (n *nodeMetadataHandler) PatchAnnotationMetadata(node *v1.Node, key string, value any) (map[string]any, bool) {
	if val, present := node.GetObjectMeta().GetAnnotations()[key]; !present && value == nil || val == value {
		return nil, false
	}
	patch := map[string]any{ // Node
		"metadata": map[string]any{ // ObjectMeta
			"annotations": map[string]any{
				key: value,
			},
		},
	}
	return patch, true
}
func (n *nodeMetadataHandler) ChangeMetadata(node *v1.Node, op PatchOperation, key string, value any) (bool, error) {
	patch, complete := op(node, key, value)
	if !complete {
		return true, nil
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return false, err
	}
	// Add metadata to node using Patch with JSON Merge Patch
	_, err = n.client.CoreV1().Nodes().Patch(context.Background(), node.Name, types.MergePatchType, data, metav1.PatchOptions{FieldManager: FieldManager})
	if err != nil {
		return false, err
	}
	return false, nil
}
func (n *nodeMetadataHandler) ApplyLabel(node *v1.Node, key, value string) (bool, error) {
	return n.ChangeMetadata(node, n.PatchLabelMetadata, key, value)
}
func (n *nodeMetadataHandler) RemoveLabel(node *v1.Node, key string) (bool, error) {
	return n.ChangeMetadata(node, n.PatchLabelMetadata, key, nil)
}
func (n *nodeMetadataHandler) ApplyAnnotation(node *v1.Node, key, value string) (bool, error) {
	return n.ChangeMetadata(node, n.PatchAnnotationMetadata, key, value)
}
func (n *nodeMetadataHandler) RemoveAnnotation(node *v1.Node, key string) (bool, error) {
	return n.ChangeMetadata(node, n.PatchAnnotationMetadata, key, nil)
}
Powered by Gitiles| Privacy| Terms