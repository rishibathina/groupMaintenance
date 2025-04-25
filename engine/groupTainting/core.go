// Get the VM instance of the node
// Returns strings in the order of: projectID, Zone, instanceName
func getVMInfo(node *v1.Node) (string, string, string) {
	providerID := node.Spec.ProviderID
	if !strings.HasPrefix(providerID, "gce://") {
		return "", "", ""
	}

	parts := strings.Split(strings.TrimPrefix(providerID, "gce://"), "/")
	if len(parts) != 3 {
		return "", "", ""
	}

	return parts[0], parts[1], parts[2]
}