// Package all blank-imports every deployment provider so their init() functions
// register them in the provider registry. The service (and any binary that
// wants the full catalogue) imports this package for its side effects; nothing
// here is called directly. Adding a provider means adding one import line.
package all

import (
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/awsacm"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/bigip"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/cloudflare"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/dummy"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/fortigate"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/webhook"
)
