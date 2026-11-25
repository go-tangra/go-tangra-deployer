package jobs

// AggregateParent exposes the parent aggregation to the external tests (it
// runs from every finishing child and from claimed parents).
var AggregateParent = (*Service).aggregateParent
