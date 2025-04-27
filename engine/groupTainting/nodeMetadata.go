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

func (n *nodeMetadataHandler) PatchTaintMetadata(node *v1.Node, key string, value any) (map[string]any, bool) {
	var newTaints []v1.Taint
	switch opDetails := value.(type) {
	case corev1.Taint: 
		taintToApply := opDetails
		exactMatchExists := false
		for _, t := range currentTaints {
			if t.Key == taintToApply.Key && t.Effect == taintToApply.Effect {
				return nil, false
			}
		}
		
		newTaints = append(node.Spec.Taints, taintToApply)

	case *corev1.TaintEffect: 
		newTaints = removeTaintKey(key, node.Spec.Taints)

	default:
		log.Printf("Error: Invalid operationDetails type (%T) for PatchTaintMetadata with key '%s'", operationDetails, taintKeyToMatch)
		return nil, false
	}

	patch := map[string]any{
		"spec": map[string]any{
			"taints": newTaints,
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

func (n *nodeMetadataHandler) ApplyTaint(node *v1.Node, taintToApply corev1.Taint) (bool, error) {
	return n.ChangeMetadata(node, n.PatchTaintMetadata, taintToApply.Key, taintToApply)
}

func (n *nodeMetadataHandler) RemoveTaint(node *v1.Node, keyToRemove string) (bool, error) {
	var nilEffectPtr *corev1.TaintEffect
	return n.ChangeMetadata(node, n.PatchTaintMetadata, keyToRemove, nilEffectPtr)
}

func removeTaintKey(searchTaintKey string, taints []v1.Taint) []v1.Taint {
	index := getTaintIndex(searchTaintKey, taints)

	if index > -1 {
		return append(taints[:index], taints[index+1:]...)
	}
	return taints
}

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