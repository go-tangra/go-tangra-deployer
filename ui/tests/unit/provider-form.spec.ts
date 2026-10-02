// T084: the schema-driven configuration drawer (contracts/deployer-config-ui.md §6).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import Configurations from '@/views/configurations/index.vue'
import { answer, axeViolations, body, catalogue, choose, click, drawer, FakeSource, fetchMock, mkRouter, page, q, refuse, type, type Handler } from './helpers'

const global = { plugins: [mkRouter()] }
const ZONE = '023e105f4ecef8ad9ca31a8372d0c353'

/** A deployer API: the catalogue, a list of configurations and single reads. */
function api(rows: Record<string, unknown>[] = [], extra: Handler = () => undefined): Handler {
  return (url, init) => {
    const e = extra(url, init)
    if (e !== undefined) return e
    const path = url.split('?')[0]!
    if (path.endsWith('/providers')) return catalogue
    if (path.endsWith('/configurations') && (init.method ?? 'GET') === 'GET') return page(rows)
    const one = rows.find((r) => path.endsWith('/configurations/' + String(r.id)))
    if (one && (init.method ?? 'GET') === 'GET') return one
    if (init.method === 'POST' || init.method === 'PUT') return { id: 'new', name: 'x', provider_type: 'x', status: 'active', has_credentials: false }
    return page([])
  }
}

let w: VueWrapper | null = null
async function mountView(handler: Handler) {
  const calls = fetchMock(handler)
  w = mount(Configurations, { global, attachTo: document.body })
  await flushPromises()
  return calls
}
async function openNew(): Promise<void> {
  await w!.find('[data-test=config-new]').trigger('click')
  await flushPromises()
}
async function openRow(id: string): Promise<void> {
  await w!.find(`[data-test="config-row-${id}"]`).trigger('click')
  await flushPromises()
}
const input = (id: string) => q<HTMLInputElement>(`[id="${id}"]`)
const label = (id: string) => q<HTMLLabelElement>(`label[for="${id}"]`)?.textContent ?? ''
const hintOf = (id: string) => {
  const ref = input(id)?.getAttribute('aria-describedby')
  return ref ? document.getElementById(ref)?.textContent ?? '' : ''
}
const save = () => click(q('[data-test=config-save]'))
const sectionTitles = () => Array.from(drawer().querySelectorAll('[data-section]')).map((s) => s.getAttribute('data-section'))

describe('configuration drawer: schema-driven provider form', () => {
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

  it('provider first; its fields with defaults, help, placeholders, required markers and sections; no JSON text areas', async () => {
    await mountView(api())
    await openNew()
    expect(q('[data-provider]')).toBeNull()
    expect(q('[data-test=choose-provider]')).not.toBeNull()
    expect(drawer().querySelector('select')!.id).toBe('provider_type') // the first input
    await choose(q('#provider_type'), 'bigip')
    expect(q('[data-provider=bigip]')).not.toBeNull()
    expect(sectionTitles()).toEqual(['connection', 'credentials', 'options']) // ssl_profile (US8) lives in Options
    expect(input('config.partition')!.value).toBe('Common')
    expect(input('credentials.host')!.placeholder).toBe('bigip.example.com')
    expect(hintOf('credentials.host')).toContain('Management address')
    expect(label('credentials.password')).toContain('*')
    expect(input('credentials.password')!.type).toBe('password')
    expect(input('credentials.password')!.autocomplete).toBe('off')
    expect(input('credentials.username')!.autocomplete).toBe('off')
    expect(q('textarea[data-field=config]')).toBeNull()
    expect(q('textarea[data-field=credentials]')).toBeNull()
    expect(q('[data-test=config-validate]')!.textContent).toContain('Test connection')
  })

  it('each descriptor type renders its kit input; Options collapsed at its defaults', async () => {
    await mountView(api())
    await openNew()
    await choose(q('#provider_type'), 'webhook')
    expect(sectionTitles()).toEqual(['connection', 'credentials', 'options'])
    expect(input('config.url')!.type).toBe('url')
    expect(input('config.timeout_seconds')!.type).toBe('number')
    expect(input('config.timeout_seconds')!.value).toBe('60')
    expect(input('config.skip_tls_verify')!.getAttribute('role')).toBe('switch')
    expect(input('config.headers')!.placeholder).toBe('key=value') // UiTagEditor
    expect(input('credentials.token')!.type).toBe('password')
    expect(q<HTMLDetailsElement>('[data-section=options] details')!.open).toBe(false)
    expect(q('[data-test=config-validate]')!.textContent).toContain('Test connection')
    await choose(q('#provider_type'), 'aws_acm') // nothing entered: no confirm
    expect(input('credentials.session_token')!.type).toBe('password') // secret text
    expect(q('[data-test=config-validate]')!.textContent).toContain('Check settings')
    await choose(q('#provider_type'), 'fortigate')
    expect(q<HTMLSelectElement>('select[id="config.import_scope"]')!.value).toBe('global') // enum
    await choose(q('#provider_type'), 'dummy')
    expect(q('[data-section=credentials]')).toBeNull()
  })

  it('required field empty: refused in the browser, highlighted and focused, nothing sent', async () => {
    const calls = await mountView(api())
    await openNew()
    await type(input('name'), 'Edge LB')
    await choose(q('#provider_type'), 'bigip')
    await type(input('credentials.host'), 'bigip.example.com')
    await type(input('credentials.username'), 'ops')
    await save()
    expect(calls.some((c) => c.init.method === 'POST')).toBe(false)
    expect(input('credentials.password')!.getAttribute('aria-invalid')).toBe('true')
    expect(document.activeElement?.id).toBe('credentials.password')
    await type(input('credentials.password'), 'pw-SECRET')
    await save()
    expect(body(calls, 'POST', '/configurations')).toEqual({ name: 'Edge LB', provider_type: 'bigip', config: { partition: 'Common' }, credentials: { host: 'bigip.example.com', username: 'ops', password: 'pw-SECRET' } })
  })

  it('switching provider with values asks to discard; cancel restores the previous provider and values', async () => {
    await mountView(api())
    await openNew()
    await choose(q('#provider_type'), 'cloudflare')
    await type(input('config.zone_id'), ZONE)
    await choose(q('#provider_type'), 'bigip')
    answer(false)
    await flushPromises()
    expect(q<HTMLSelectElement>('#provider_type')!.value).toBe('cloudflare')
    expect(input('config.zone_id')!.value).toBe(ZONE)
    await choose(q('#provider_type'), 'bigip')
    answer(true)
    await flushPromises()
    expect(q('[data-provider=bigip]')).not.toBeNull()
    expect(input('config.zone_id')).toBeNull()
  })

  it('edit: pre-filled values and credentials_public, secrets blank "Stored — leave blank to keep", provider fixed; blank secret omitted', async () => {
    const row = { id: 'c1', name: 'Edge', provider_type: 'bigip', status: 'active', has_credentials: true, config: { partition: 'Prod' }, credentials_set: ['host', 'password', 'username'], credentials_public: { host: 'bigip.example.com', username: 'ops' }, target_supplied: [] }
    const calls = await mountView(api([row]))
    await openRow('c1')
    expect(calls.some((c) => c.url.endsWith('/configurations/c1') && (c.init.method ?? 'GET') === 'GET')).toBe(true)
    expect(q<HTMLSelectElement>('#provider_type')!.disabled).toBe(true)
    expect(input('config.partition')!.value).toBe('Prod')
    expect(input('credentials.host')!.value).toBe('bigip.example.com')
    expect(input('credentials.password')!.value).toBe('')
    expect(hintOf('credentials.password')).toContain('Stored — leave blank to keep')
    expect(label('credentials.password')).not.toContain('*') // stored: not required again
    await type(input('config.partition'), 'Common')
    await save()
    expect(body(calls, 'PUT', '/configurations/c1')).toEqual({ name: 'Edge', provider_type: 'bigip', config: { partition: 'Common' }, credentials: { host: 'bigip.example.com', username: 'ops' } })
  })

  it('edit: Clear on an optional stored secret sends clear_credentials; Test connection sends configuration_id after client validation', async () => {
    const row = { id: 'w1', name: 'Hook', provider_type: 'webhook', status: 'active', has_credentials: true, config: { url: 'https://hooks.example.com/c', timeout_seconds: 30 }, credentials_set: ['token'], credentials_public: {}, target_supplied: [] }
    const calls = await mountView(api([row], (url) => (url.endsWith('/configurations/validate') ? { valid: true, checked: 'probe', deferred: [] } : undefined)))
    await openRow('w1')
    expect(q<HTMLDetailsElement>('[data-section=options] details')!.open).toBe(true) // timeout differs from its default
    await click(q('[data-test=clear-token]'))
    expect(hintOf('credentials.token')).toContain('Removed when you save')
    await click(q('[data-test=config-validate]'))
    expect(body(calls, 'POST', '/configurations/validate')).toMatchObject({ provider_type: 'webhook', configuration_id: 'w1', config: { url: 'https://hooks.example.com/c', timeout_seconds: 30 } })
    expect(q('[data-test=validate-result]')!.textContent).toContain('Connection successful')
    await type(input('config.url'), 'ftp://nope')
    const before = calls.length
    await click(q('[data-test=config-validate]'))
    expect(calls.length).toBe(before) // the client-side schema refuses first
    expect(input('config.url')!.getAttribute('aria-invalid')).toBe('true')
    await type(input('config.url'), 'https://hooks.example.com/c')
    await save()
    expect(body(calls, 'PUT', '/configurations/w1')).toMatchObject({ clear_credentials: ['token'] })
  })

  it('server 422 detail.fields → inline errors and focus; credentials_rejected → alert; secrets never printed', async () => {
    const calls = await mountView(api([], (url, init) => {
      if (url.endsWith('/configurations/validate')) return refuse(422, { reason: 'credentials_rejected' })
      if (url.endsWith('/configurations') && init.method === 'POST') return refuse(422, { reason: 'validation_failed', detail: { fields: { 'config.partition': 'pattern', 'credentials.host': 'too_long:270' } } })
      return undefined
    }))
    await openNew()
    await type(input('name'), 'LB')
    await choose(q('#provider_type'), 'bigip')
    await type(input('credentials.host'), 'h.example')
    await type(input('credentials.username'), 'u')
    await type(input('credentials.password'), 'TOP-SECRET-PW')
    await click(q('[data-test=config-validate]'))
    expect(q('[data-test=validate-result]')!.textContent).toContain('refused')
    await save()
    expect(calls.filter((c) => c.init.method === 'POST' && c.url.endsWith('/configurations')).length).toBe(1)
    expect(hintOf('config.partition')).toContain('wrong format')
    expect(hintOf('credentials.host')).toContain('at most 270')
    expect(document.activeElement?.id).toBe('config.partition')
    expect(drawer().textContent).not.toContain('TOP-SECRET-PW')
  })

  it('legacy undeclared keys are listed and dropped on save; a missing required field is highlighted on open', async () => {
    const row = { id: 'l1', name: 'Old', provider_type: 'fortigate', status: 'active', has_credentials: true, config: { vdom: '', replace_strategy: 'x', import_scope: 'vdom' }, credentials_set: ['host', 'api_token'], credentials_public: { host: 'fw.example' }, target_supplied: ['vdom'] }
    const calls = await mountView(api([row]))
    await openRow('l1')
    expect(q('[data-test=legacy-keys]')!.textContent).toContain('replace_strategy')
    await type(input('config.vdom'), 'root')
    await save()
    expect(body(calls, 'PUT', '/configurations/l1')!.config).toEqual({ vdom: 'root', import_scope: 'vdom' })
  })

  it('read-only view for readers: labelled values and "stored" badges for secrets', async () => {
    const row = { id: 'r1', name: 'RO', provider_type: 'cloudflare', status: 'active', has_credentials: true, config: { zone_id: ZONE }, target_supplied: [] }
    await mountView(api([row]))
    await openRow('r1')
    expect(q('[data-test=config-view]')!.textContent).toContain(ZONE)
    expect(q('[data-test=secret-stored]')!.textContent).toContain('stored')
    expect(q('[data-test=config-save]')).toBeNull()
  })

  it('overridable required field: hint, saved empty with the warning, list badge and Deploy disabled; required_by_targets names the targets', async () => {
    const shared = { id: 's1', name: 'Shared CF', provider_type: 'cloudflare', status: 'active', has_credentials: true, config: {}, target_supplied: ['zone_id'], credentials_set: ['api_token'], credentials_public: {} }
    const calls = await mountView(api([shared], (url, init) => (url.endsWith('/configurations/s1') && init.method === 'PUT' ? refuse(422, { reason: 'validation_failed', detail: { fields: { 'config.zone_id': 'required_by_targets' }, targets: [{ id: 't1', name: 'edge-zone-b' }] } }) : undefined)))
    expect(w!.find('[data-test=needs-target-values]').text()).toBe('Needs target values: Zone ID')
    expect((w!.find('[data-test=config-deploy-s1]').element as HTMLButtonElement).disabled).toBe(true)
    await openNew()
    await type(input('name'), 'CF2')
    await choose(q('#provider_type'), 'cloudflare')
    expect(hintOf('config.zone_id')).toContain('Required — or leave empty and let each target provide it')
    await type(input('credentials.api_token'), 'tok')
    expect(q('[data-test=target-supplied-warning]')!.textContent).toContain('Zone ID')
    await save()
    expect(body(calls, 'POST', '/configurations')).toEqual({ name: 'CF2', provider_type: 'cloudflare', config: {}, credentials: { api_token: 'tok' } })
    // Editing the shared one: the server refuses (targets rely on it).
    await openRow('s1')
    expect(hintOf('config.zone_id')).toContain('To be provided by each target')
    await type(input('config.zone_id'), '')
    await save()
    expect(hintOf('config.zone_id')).toContain('edge-zone-b')
  })

  it('axe: create and edit drawers are clean', async () => {
    const row = { id: 'c1', name: 'Edge', provider_type: 'webhook', status: 'active', has_credentials: true, config: { url: 'https://h.example/x' }, credentials_set: ['token'], credentials_public: {}, target_supplied: [] }
    await mountView(api([row]))
    await openNew()
    await choose(q('#provider_type'), 'webhook')
    expect(await axeViolations(drawer())).toEqual([])
    await openRow('c1')
    expect(await axeViolations(drawer())).toEqual([])
    expect(drawer().querySelector('[style]')).toBeNull()
  })
})
