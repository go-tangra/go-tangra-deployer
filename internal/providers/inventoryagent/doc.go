// Package inventoryagent is the deployment provider "inventory-agent" (feature
// 033): it delivers a certificate to inventory hosts through their inventory
// agents, which install it in the certbot layout.
//
// Security role: the provider delivers by reference. It never fetches, holds
// or forwards a private key (Capabilities.DeliversByReference makes the job
// scheduler fetch the certificate without its key). It sends the inventory
// module only references — tenant, deployer job id, configuration and target
// ids, the lcm certificate id, the install name and the host selection — over
// the SPIFFE-mTLS mesh (inventory.v1.CertificateDeliveryService). The
// inventory resolves the hosts within that tenant and the agents pull the
// material from it over their own authenticated connections. The install name
// is validated here (a single safe path component) and again by the agent.
package inventoryagent
