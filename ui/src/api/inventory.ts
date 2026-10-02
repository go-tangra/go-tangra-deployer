// Inventory API calls of the host picker (feature 033, US6). They go through
// the gateway with the signed-in user's session, so they show exactly the
// hosts the user may read (inventory:read); agent state needs agents:manage
// and is optional.
import { api, ApiError } from '@/api/client'
import type { Page } from '@/stores/paged'

export const INVENTORY = '/api/inventory/v1'

export interface InventoryHost {
  id: string
  hostname: string
  os_name?: string
  os_version?: string
  status?: string
  tags?: Record<string, string>
}

export interface InventoryAgent {
  host_id?: string
  hostname?: string
  online?: boolean
  certificate_capability?: string
}

export interface HostQuery {
  page: number
  page_size: number
  hostname?: string | undefined
  tag?: string | undefined
}

/** One page of inventory hosts (hostname order). */
export async function listHosts(q: HostQuery, signal?: AbortSignal): Promise<Page<InventoryHost>> {
  return api<Page<InventoryHost>>('GET', INVENTORY + '/hosts', undefined, { query: { ...q, sort: 'hostname', order: 'asc' }, ...(signal ? { signal } : {}) })
}

/** One host (chips of hosts selected before the page was loaded). */
export async function getHost(id: string): Promise<InventoryHost> {
  return api<InventoryHost>('GET', INVENTORY + '/hosts/' + encodeURIComponent(id))
}

/** Agent state by host id (online, certificate capability); up to `max` agents. */
export async function agentsByHost(max = 1000): Promise<Map<string, InventoryAgent>> {
  const out = new Map<string, InventoryAgent>()
  for (let page = 1; out.size < max; page++) {
    const res = await api<Page<InventoryAgent>>('GET', INVENTORY + '/agents', undefined, { query: { page, page_size: 200, sort: 'hostname', order: 'asc' } })
    for (const a of res.items ?? []) if (a.host_id) out.set(a.host_id, a)
    if (!res.items?.length || page * 200 >= (res.total ?? 0)) break
  }
  return out
}

/** The user cannot read the inventory (not signed in for it, no permission, module absent). */
export function unavailable(e: unknown): boolean {
  return e instanceof ApiError && [401, 403, 404].includes(e.status)
}

/** Wording and colour of an agent's certificate capability (inventory data-model §1.5). */
export const CAPABILITY: Record<string, { text: string; color: 'success' | 'warning' | 'neutral' | 'error' }> = {
  enabled: { text: 'Certificates ready', color: 'success' },
  disabled_on_host: { text: 'Disabled on host', color: 'warning' },
  upgrade_required: { text: 'Agent upgrade required', color: 'warning' },
  not_supported_platform: { text: 'Platform not supported', color: 'neutral' },
  disabled_on_server: { text: 'Disabled by the server', color: 'neutral' },
}
