
sharedTriggerNodeCache := &sync.Map{}

opportunisticHandler := opportunisticmaintenance.NewOpportunsiticHandler(
	mgr.GetClient(),
	enableLeaderElection,
	false,
	sharedTriggerNodeCache,
)