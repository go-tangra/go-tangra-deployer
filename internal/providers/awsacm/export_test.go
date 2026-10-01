package awsacm

import "github.com/go-tangra/go-tangra-deployer/v4/internal/provider"

// WithTestEndpoint returns a provider that talks to url instead of AWS. It
// exists only in test builds; production code cannot redirect the provider.
func WithTestEndpoint(url string) provider.Provider { return Provider{endpoint: url} }
