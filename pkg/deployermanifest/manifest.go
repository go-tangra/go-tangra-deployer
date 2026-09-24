// Package deployermanifest declares what the deployer module registers with the
// application gateway: routes derived from the embedded OpenAPI document, the
// API permissions, the CASL abilities and the navigation entries. The
// deployer.v1 gRPC surface is service-to-service and not proxied.
package deployermanifest

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/go-tangra/go-tangra-deployer/v4/api/openapi"
	"github.com/go-tangra/go-tangra-portal/sdk/v4/pkg/gatewayclient"
)

// Module identity.
const (
	Module       = "deployer"
	DisplayName  = "Deployer"
	Version      = "1.0.0"
	RemotePrefix = "/ui"
)

// OpenAPI operation extensions.
const (
	PermissionExtension = "x-freya-permission"
	PublicExtension     = "x-freya-public"
	BodyLimitExtension  = "x-freya-max-body-bytes"
	TimeoutExtension    = "x-freya-timeout-seconds"
)

// Permissions the module registers.
var Permissions = []gatewayclient.Permission{
	{Resource: "configurations", Action: "read", Description: "List and read deployment endpoint configurations (credentials redacted)"},
	{Resource: "configurations", Action: "manage", Description: "Create, change, delete and validate deployment configurations"},
	{Resource: "targets", Action: "read", Description: "List and read deployment targets and their filters"},
	{Resource: "targets", Action: "manage", Description: "Create, change and delete deployment targets and attachments"},
	{Resource: "jobs", Action: "read", Description: "List and read deployment jobs and their history"},
	{Resource: "jobs", Action: "manage", Description: "Cancel and retry deployment jobs"},
	{Resource: "deploy", Action: "execute", Description: "Deploy, verify and roll back certificates to endpoints"},
	{Resource: "stats", Action: "read", Description: "Read deployment statistics"},
	{Resource: "backup", Action: "manage", Description: "Export and import tenant deployment configuration"},
}

// Grants maps built-in role slugs to the permissions they hold.
var Grants = map[string][]string{
	"owner":    PermissionRefs(),
	"admin":    PermissionRefs(),
	"member":   {"configurations:read", "targets:read", "jobs:read", "stats:read"},
	"auditor":  {"stats:read", "jobs:read"},
	"operator": {"configurations:read", "configurations:manage", "targets:read", "targets:manage", "jobs:read", "jobs:manage", "deploy:execute", "stats:read"},
}

// Methods proxied by the gateway: none (deployer.v1 is service to service).
var Methods []gatewayclient.Method

// Abilities are the CASL rules bound to the permissions.
var Abilities = []gatewayclient.Ability{
	{Action: []string{"read", "create", "update", "delete"}, Subject: []string{"DeployerConfiguration"}, Requires: "configurations:read"},
	{Action: []string{"read", "create", "update", "delete"}, Subject: []string{"DeployerTarget"}, Requires: "targets:read"},
	{Action: []string{"read", "manage"}, Subject: []string{"DeployerJob"}, Requires: "jobs:read"},
	{Action: []string{"execute"}, Subject: []string{"Deployment"}, Requires: "deploy:execute"},
	{Action: []string{"read"}, Subject: []string{"DeployerStats"}, Requires: "stats:read"},
	{Action: []string{"manage"}, Subject: []string{"DeployerBackup"}, Requires: "backup:manage"},
}

// Nav lists the navigation contributions.
var Nav = []gatewayclient.NavEntry{
	{Title: "Dashboard", Path: "/deployer", Icon: "mdi-view-dashboard-outline", Order: 400, Requires: "stats:read"},
	{Title: "Targets", Path: "/deployer/targets", Icon: "mdi-target", Order: 410, Requires: "targets:read"},
	{Title: "Configurations", Path: "/deployer/configurations", Icon: "mdi-cog-outline", Order: 420, Requires: "configurations:read"},
	{Title: "Jobs", Path: "/deployer/jobs", Icon: "mdi-progress-clock", Order: 430, Requires: "jobs:read"},
}

// PermissionRefs lists "resource:action" for every declared permission.
func PermissionRefs() []string {
	out := make([]string, 0, len(Permissions))
	for _, p := range Permissions {
		out = append(out, p.Resource+":"+p.Action)
	}
	return out
}

// Routes derives the gateway routes from the OpenAPI document.
func Routes(doc *openapi3.T) ([]gatewayclient.Route, error) {
	known := map[string]bool{}
	for _, p := range PermissionRefs() {
		known[p] = true
	}
	var routes []gatewayclient.Route
	for p, item := range doc.Paths.Map() {
		for m, op := range item.Operations() {
			r := gatewayclient.Route{Method: strings.ToUpper(m), Path: p}
			perm, _ := op.Extensions[PermissionExtension].(string)
			public, _ := op.Extensions[PublicExtension].(bool)
			switch {
			case public:
				r.Public = true
			case perm == "":
				return nil, fmt.Errorf("deployermanifest: %s %s declares no permission", r.Method, p)
			case !known[perm]:
				return nil, fmt.Errorf("deployermanifest: %s %s uses undeclared permission %q", r.Method, p, perm)
			default:
				r.Permission = perm
			}
			if v, ok := op.Extensions[BodyLimitExtension]; ok {
				n, ok := v.(float64)
				if !ok || n <= 0 {
					return nil, fmt.Errorf("deployermanifest: %s %s has a bad body limit", r.Method, p)
				}
				r.MaxBodyBytes = uint64(n)
			}
			if v, ok := op.Extensions[TimeoutExtension]; ok {
				n, ok := v.(float64)
				if !ok || n <= 0 || n > 600 {
					return nil, fmt.Errorf("deployermanifest: %s %s has a bad timeout", r.Method, p)
				}
				r.Timeout = time.Duration(n) * time.Second
			}
			routes = append(routes, r)
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].Method+" "+routes[i].Path < routes[j].Method+" "+routes[j].Path })
	return routes, nil
}

// Load parses the embedded document.
func Load() (*openapi3.T, error) {
	return openapi3.NewLoader().LoadFromData(openapi.Deployer)
}

// Manifest builds the gateway manifest from the embedded OpenAPI document.
func Manifest() (gatewayclient.Manifest, error) {
	doc, err := Load()
	if err != nil {
		return gatewayclient.Manifest{}, err
	}
	routes, err := Routes(doc)
	if err != nil {
		return gatewayclient.Manifest{}, err
	}
	return gatewayclient.Manifest{
		Module: Module, DisplayName: DisplayName, Version: Version,
		Prefixes:    []string{"/api/deployer"},
		Routes:      routes,
		Methods:     Methods,
		Permissions: Permissions,
		Abilities:   Abilities,
		Exposes:     []string{"./routes", "./nav"},
		Nav:         Nav,
	}, nil
}
