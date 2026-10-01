package cloudflare

import "github.com/go-tangra/go-tangra-deployer/v4/internal/provider"

// WithTestAPIBase returns a provider that talks to url instead of the
// Cloudflare API. It exists only in test builds; production code cannot
// redirect the provider.
func WithTestAPIBase(url string) provider.Provider { return Provider{apiBase: url} }
