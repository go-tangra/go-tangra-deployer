// T095: the inventory-agent configuration through the generic form
// (ProviderConfigForm + HostPicker in the field-host_ids slot), against
// contracts/deployer-provider.md §2.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import Configurations from '@/views/configurations/index.vue'
import { fieldsToZod, overrideToZod } from '@/schemas/providerFields'
import { body, catalogue, choose, click, FakeSource, fetchMock, mkRouter, page, provider, q, type, type Handler } from './helpers'

const H1 = '0192a7c0-0000-7000-8000-000000000001'
const H2 = '0192a7c0-0000-7000-8000-000000000002'
const hosts = [{ id: H1, hostname: 'web-1', os_name: 'Debian', tags: { role: 'web' } }, { id: H2, hostname: 'web-2', os_name: 'Ubuntu', tags: {} }]
const stored = { id: 'i1', name: 'Fleet', provider_type: 'inventory-agent', status: 'active', has_credentials: false, config: { host_ids: [H2], host_tags: ['role=web'], cert_name: 'www', key_policy: 'certificate_only', require_all_success: true, wait_seconds: 120 }, credentials_set: [], target_supplied: [] }

function api(extra: Handler = () => undefined): Handler {
  return (url, init) => {
    const e = extra(url, init)
    if (e !== undefined) return e
    const path = url.split('?')[0]!
    const m = init.method ?? 'GET'
    if (path.endsWith('/providers')) return catalogue
    if (path === '/api/inventory/v1/hosts') return { ...page(hosts), page_size: 10 }
    if (path.startsWith('/api/inventory/v1/hosts/')) return hosts.find((h) => path.endsWith(h.id)) ?? hosts[0]
    if (path === '/api/inventory/v1/agents') return page([{ host_id: H1, online: true, certificate_capability: 'enabled' }])
    if (path.endsWith('/configurations/i1') && m === 'GET') return stored
    if (path.endsWith('/configurations') && m === 'GET') return page([stored])
    if (path.endsWith('/configurations/validate')) return { valid: true, checked: 'static', deferred: [], details: { matched_hosts: [{ host_id: H1, hostname: 'web-1', os_name: 'Debian', tags: { role: 'web' }, agent_online: true, capability: 'enabled' }], unknown_host_ids: [], truncated: false } }
    return { id: 'new', name: 'x', provider_type: 'inventory-agent', status: 'active', has_credentials: false }
  }
}

let w: VueWrapper | null = null
async function mountView(handler: Handler) {
  const calls = fetchMock(handler)
  w = mount(Configurations, { global: { plugins: [mkRouter()] }, attachTo: document.body })
  await flushPromises()
  return calls
}
const input = (id: string) => q<HTMLInputElement>(`[id="${id}"]`)

describe('inventory-agent configuration', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    vi.stubGlobal('EventSource', FakeSource)
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
  })
  afterEach(() => {
    w?.unmount()
    w = null
  })

  it('create: host picker in the host_ids slot, no Credentials section, "Preview hosts"; payload equals the config contract', async () => {
    const calls = await mountView(api())
    await w!.find('[data-test=config-new]').trigger('click')
    await flushPromises()
    await type(input('name'), 'Web fleet')
    await choose(q('#provider_type'), 'inventory-agent')
    expect(q('[data-test=host-picker]')).not.toBeNull()
    expect(q('[data-section=credentials]')).toBeNull()
    expect(q('[data-test=config-validate]')!.textContent).toContain('Preview hosts')
    const box = q<HTMLInputElement>(`[data-test="host-row-${H1}"] input[type=checkbox]`)!
    box.checked = true
    box.dispatchEvent(new Event('change'))
    await flushPromises()
    expect(q('[data-test=host-chips]')!.textContent).toContain('web-1')
    const tags = input('config.host_tags')!
    await type(tags, 'env=prod')
    tags.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter' }))
    await flushPromises()
    await click(q('[data-test=options-toggle]'))
    await type(input('config.cert_name'), 'www')
    await choose(q('select[id="config.key_policy"]'), 'certificate_only')
    await type(input('config.wait_seconds'), '30')
    await click(q('[data-test=config-save]'))
    expect(body(calls, 'POST', '/configurations')).toEqual({ name: 'Web fleet', provider_type: 'inventory-agent', config: { host_ids: [H1], host_tags: ['env=prod'], cert_name: 'www', key_policy: 'certificate_only', require_all_success: false, wait_seconds: 30 } })
  })

  it('edit round-trip: the stored config comes back unchanged; Preview hosts lists matched_hosts', async () => {
    const calls = await mountView(api())
    await w!.find('[data-test="config-row-i1"]').trigger('click')
    await flushPromises()
    expect(q('[data-test=host-chips]')!.textContent).toContain('web-2') // single read for the chip name
    await click(q('[data-test=config-validate]'))
    expect(body(calls, 'POST', '/configurations/validate')).toMatchObject({ provider_type: 'inventory-agent', configuration_id: 'i1', config: stored.config })
    const table = q('[data-test=matched-hosts]')!
    expect(table.textContent).toContain('web-1')
    expect(table.textContent).toContain('Certificates ready')
    expect(q('[data-test=validate-result]')!.textContent).toContain('1 host matches the selection')
    await click(q('[data-test=config-save]'))
    expect(body(calls, 'PUT', '/configurations/i1')!.config).toEqual(stored.config)
  })

  it('the descriptor-derived schema mirrors the config contract', () => {
    const p = provider('inventory-agent')
    const codes = (r: { success: boolean; error?: { issues: readonly { path: PropertyKey[]; message: string }[] } | undefined }) => Object.fromEntries((r.error?.issues ?? []).map((i) => [i.path.join('.'), i.message]))
    const eff = fieldsToZod(p, { mode: 'effective' })
    expect(codes(eff.safeParse({ config: { host_tags: ['a'], cert_name: 'bad name' } }))).toEqual({ 'config.cert_name': expect.stringContaining('wrong format') })
    expect(codes(eff.safeParse({ config: { host_tags: Array.from({ length: 17 }, (_, i) => 't' + i) } }))).toEqual({ 'config.host_tags': 'At most 16 entries.' })
    const none = codes(eff.safeParse({ config: {} }))
    expect(none['config.host_ids']).toMatch(/select hosts or enter host tags/i)
    expect(none['config.host_tags']).toMatch(/select hosts or enter host tags/i)
    expect(codes(eff.safeParse({ config: { host_ids: [H1], wait_seconds: 241, key_policy: 'x' } }))).toEqual({ 'config.wait_seconds': 'Must be between 0 and 240.', 'config.key_policy': 'Choose one of the listed values.' })
    // A configuration may leave the whole host selection to its targets.
    const cfg = fieldsToZod(p).safeParse({ config: { cert_name: 'www' } })
    expect(cfg.success && cfg.data.target_supplied).toEqual(['host_ids', 'host_tags'])
    // …which each target must then supply.
    expect(codes(overrideToZod(p, { cert_name: 'www' }).safeParse({}))['config_overrides.host_ids']).toMatch(/select hosts/i)
    expect(overrideToZod(p, { cert_name: 'www' }).parse({ host_tags: ['role=web'], require_all_success: true })).toEqual({ host_tags: ['role=web'], require_all_success: true })
  })
})
