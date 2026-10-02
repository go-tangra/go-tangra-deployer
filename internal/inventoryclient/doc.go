// Package inventoryclient adapts the inventory SDK certificate-delivery client
// (github.com/go-tangra/go-tangra-inventory/sdk/v4/pkg/inventoryclient) to the
// deployer: it is built over the Freya mesh connection to the inventory
// service (SPIFFE mTLS; the inventory accepts these calls only from the
// deployer's service identity) and bounds every call with a deadline.
//
// Security role: only references cross this boundary — tenant, job and
// configuration ids, the lcm certificate id, the install name, host ids and
// tags, serials and fingerprints. No certificate or key material is ever sent
// or received.
package inventoryclient
