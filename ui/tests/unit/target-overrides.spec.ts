// T085: the target form renders one override form per attached configuration
// from the provider descriptors (contracts/deployer-config-ui.md §4a, §6.11).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import Targets from '@/views/targets/index.vue'
import { axeViolations, body, catalogue, click, drawer, FakeSource, fetchMock, mkRouter, page, q, refuse, type, type Call, type Handler } from './helpers'

const ZONE = '023e105f4ecef8ad9ca31a8372d0c353'
const cf = { id: 'cf', name: 'Shared CF', provider_type: 'cloudflare', status: 'active', has_credentials: true, config: {}, target_supplied: ['zone_id'] }
const wh = { id: 'wh', name: 'Hook', provider_type: 'webhook', status: 'active', has_credentials: false, config: { url: 'https://h.example/x', timeout_seconds: 60 }, target_supplied: [] }
const inv = { id: 'inv', name: 'Fleet', provider_type: 'inventory-agent', status: 'active', has_credentials: false, config: { key_policy: 'require' }, target_supplied: ['host_ids', 'host_tags'] }
const t1 = { id: 't1', name: 'Edge', auto_deploy: true, certificate_filters: [], configuration_ids: ['cf'], config_overrides: { cf: { zone_id: ZONE } }, missing_required: { cf: [] } }

function api(extra: Handler = () => undefined): Handler {
  return (url, init) => {
    const e = extra(url, init)
    if (e !== undefined) return e
    const path = url.split('?')[0]!
    const m = init.method ?? 'GET'
    if (path.endsWith('/providers')) return catalogue
    if (path.startsWith('/api/inventory/')) return refuse(403, { reason: 'forbidden' })
    if (path.endsWith('/configurations') && m === 'GET') return page([cf, wh, inv])
    if (path.endsWith('/targets/t1') && m === 'GET') return t1
    if (path.endsWith('/targets') && m === 'GET') return page([t1])
    if (path.endsWith('/targets') && m === 'POST') return { ...t1, id: 'new', configuration_ids: [] }
    if (m === 'PUT') return t1
    return 204
  }
}

let w: VueWrapper | null = null
async function mountView(handler: Handler): Promise<Call[]> {
  const calls = fetchMock(handler)
  w = mount(Targets, { global: { plugins: [mkRouter()] }, attachTo: document.body })
  await flushPromises()
  return calls
}
const input = (id: string) => q<HTMLInputElement>(`[id="${id}"]`)
const hintOf = (id: string) => {
  const ref = input(id)?.getAttribute('aria-describedby')
  return ref ? document.getElementById(ref)?.textContent ?? '' : ''
}
async function tick(id: string): Promise<void> {
  const box = q<HTMLInputElement>(`[id="cfg-${id}"]`)!
  box.checked = true
  box.dispatchEvent(new Event('change'))
  await flushPromises()
}
const save = () => click(q('[data-test=target-save]'))
const attaches = (calls: Call[]) => calls.filter((c) => c.init.method === 'POST' && /\/targets\/[^/]+\/configurations$/.test(c.url))

describe('target form: per-configuration override forms', () => {
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

  it('only overridable fields with inherited placeholders; target-supplied ones required; no JSON text area', async () => {
    const calls = await mountView(api())
    await w!.find('[data-test=target-new]').trigger('click')
    await flushPromises()
    expect(drawer().textContent).not.toContain('(JSON')
    await type(input('name'), 'Edge B')
    await tick('cf')
    await tick('wh')
    expect(q('[data-test=override-cf]')!.textContent).toContain('Must supply: Zone ID')
    expect(q<HTMLLabelElement>('label[for="o-cf.config_overrides.zone_id"]')!.textContent).toContain('*')
    // Webhook: url, skip_tls_verify and headers are not overridable (SR-015).
    expect(input('o-wh.config_overrides.url')).toBeNull()
    expect(input('o-wh.config_overrides.skip_tls_verify')).toBeNull()
    expect(input('o-wh.config_overrides.headers')).toBeNull()
    expect(input('o-wh.config_overrides.timeout_seconds')!.placeholder).toBe('Inherited: 60')
    // Saving without the zone id: refused in the browser, focused, nothing sent.
    await save()
    expect(calls.some((c) => c.init.method === 'POST' && c.url.endsWith('/targets'))).toBe(false)
    expect(document.activeElement?.id).toBe('o-cf.config_overrides.zone_id')
    expect(hintOf('o-cf.config_overrides.zone_id')).toContain('required')
    await type(input('o-cf.config_overrides.zone_id'), ZONE)
    await type(input('o-wh.config_overrides.timeout_seconds'), '90')
    await save()
    expect(attaches(calls).length).toBe(1)
    expect(body(calls, 'POST', '/targets/new/configurations')).toEqual({ configuration_ids: ['cf', 'wh'], config_overrides: { cf: { zone_id: ZONE }, wh: { timeout_seconds: 90 } } })
  })

  it('server 422 config_overrides.<key> + configuration_id → inline error on that configuration\'s input', async () => {
    await mountView(api((url, init) => (init.method === 'POST' && url.endsWith('/targets/t1/configurations') ? refuse(422, { reason: 'validation_failed', detail: { configuration_id: 'cf', fields: { 'config_overrides.zone_id': 'pattern' } } }) : undefined)))
    await w!.find('[data-test="target-row-t1"]').trigger('click')
    await flushPromises()
    expect(input('o-cf.config_overrides.zone_id')!.value).toBe(ZONE) // the stored override
    await type(input('o-cf.config_overrides.zone_id'), 'ABCDEFABCDEFABCDEFABCDEFABCDEF12')
    await save()
    expect(hintOf('o-cf.config_overrides.zone_id')).toContain('wrong format')
    expect(document.activeElement?.id).toBe('o-cf.config_overrides.zone_id')
  })

  it('unchanged attachments are not re-sent; legacy rows show missing_required', async () => {
    const calls = await mountView(api((url, init) => ((init.method ?? 'GET') === 'GET' && url.endsWith('/targets/t1') ? { ...t1, missing_required: { cf: ['Zone ID'] }, config_overrides: {} } : undefined)))
    await w!.find('[data-test="target-row-t1"]').trigger('click')
    await flushPromises()
    expect(q('[data-test=missing-required]')!.textContent).toContain('Zone ID')
    await type(input('o-cf.config_overrides.zone_id'), ZONE)
    await save()
    expect(body(calls, 'POST', '/targets/t1/configurations')).toEqual({ configuration_ids: ['cf'], config_overrides: { cf: { zone_id: ZONE } } })
  })

  it('inventory agent with target-supplied hosts highlights both host_ids and host_tags; axe clean', async () => {
    await mountView(api())
    await w!.find('[data-test=target-new]').trigger('click')
    await flushPromises()
    await type(input('name'), 'Web fleet')
    await tick('inv')
    expect(q('[data-test=host-picker-manual]')).not.toBeNull() // no inventory access: manual ids
    await save()
    expect(hintOf('o-inv.config_overrides.host_ids')).toMatch(/select hosts or enter host tags/i)
    expect(hintOf('o-inv.config_overrides.host_tags')).toMatch(/select hosts or enter host tags/i)
    expect(await axeViolations(drawer())).toEqual([])
  })
})
