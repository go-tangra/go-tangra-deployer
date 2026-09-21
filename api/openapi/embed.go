// Package openapi embeds the deployer browser API contract.
package openapi

import _ "embed"

// Deployer is the OpenAPI 3.1 document served and validated by the service.
//
//go:embed deployer.yaml
var Deployer []byte
