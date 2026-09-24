import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount } from '@vue/test-utils'
import { createRouter, createMemoryHistory } from 'vue-router'
import Targets from '@/views/targets/index.vue'
import Configurations from '@/views/configurations/index.vue'
import Jobs from '@/views/jobs/index.vue'
import { targetSchema, configurationSchema } from '@/schemas'

function fetchMock(handler: (url: string, init: RequestInit) => unknown) {
  const calls: { url: string; init: RequestInit }[] = []
  vi.stubGlobal('fetch', vi.fn(async (url: string, init: RequestInit = {}) => {
    calls.push({ url, init })
    const body = handler(url, init)
    if (body === 204) return new Response(null, { status: 204 })
    if (body instanceof Response) return body
    return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
  }))
  return calls
}
class FakeSource { onopen = null; onerror = null; addEventListener() {} close() {} }
const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div/>' } }] })
const global = { plugins: [router] }
const provider = { type: 'ssh', display_name: 'SSH', supports_verify: true, supports_rollback: true, required_config: ['host'], required_credentials: ['private_key'] }
const config = { id: 'c1', name: 'Edge', provider_type: 'ssh', status: 'active', has_credentials: true, config: { host: 'edge' } }
const target = { id: 't1', name: 'Edge servers', auto_deploy: true, certificate_filters: [{ common_name: '.*\\.example$' }], configuration_ids: ['c1'] }

describe('deployer schemas', () => {
  it('target: blank filters dropped, regex validated, overrides parsed; configuration: JSON objects, credentials optional', () => {
    const t = targetSchema.parse({ name: 'T', certificate_filters: [{ issuer: '', common_name: '', san: '', organization: '' }, { common_name: 'a.*' }], overrides: '{"c1":{"path":"/x"}}' })
    expect(t).toEqual({ name: 'T', auto_deploy: false, certificate_filters: [{ common_name: 'a.*' }], configuration_ids: [], overrides: { c1: { path: '/x' } } })
    const bad = targetSchema.safeParse({ name: 'T', certificate_filters: [{ common_name: '(' }], overrides: '' })
    expect(bad.success).toBe(false)
    expect(bad.success ? '' : bad.error.issues[0]!.path.join('.')).toBe('certificate_filters.0.common_name')
    expect(targetSchema.safeParse({ name: 'T', certificate_filters: [], overrides: '{"c1":"x"}' }).success).toBe(false)
    expect(configurationSchema.parse({ name: 'C', provider_type: 'ssh', config: '{"host":"h"}', credentials: '' })).toEqual({ name: 'C', provider_type: 'ssh', config: { host: 'h' }, credentials: {} })
    expect(configurationSchema.safeParse({ name: 'C', provider_type: 'ssh', config: '[1]', credentials: '' }).success).toBe(false)
  })
})

describe('deployer views on the kit', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    vi.stubGlobal('EventSource', FakeSource)
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
  })

  it('targets: drawer edits filters + attachments and reconciles them on save', async () => {
    const calls = fetchMock((url, init) => (init.method === 'PUT' ? target : init.method === 'POST' ? 204 : url.includes('/configurations') ? { items: [config, { ...config, id: 'c2', name: 'Cloud' }] } : { items: [target] }))
    const w = mount(Targets, { global, attachTo: document.body })
    await flushPromises()
    await w.find('[data-test="target-row-t1"]').trigger('click')
    await flushPromises()
    const drawer = document.body.querySelector('aside[role=dialog]')!
    expect(drawer.querySelector<HTMLInputElement>('input[data-field=name]')!.value).toBe('Edge servers')
    expect(drawer.querySelectorAll('[data-test=target-configs]').length).toBe(2)
    const cn = drawer.querySelector<HTMLInputElement>('#f-cn-0')!
    cn.value = '('
    cn.dispatchEvent(new Event('input'))
    ;(drawer.querySelector('[data-test=target-save]') as HTMLButtonElement).click()
    await flushPromises()
    expect(calls.some((c) => c.init.method === 'PUT')).toBe(false)
    expect(drawer.textContent).toContain('regular expression')
    cn.value = 'web-.*'
    cn.dispatchEvent(new Event('input'))
    const c2 = drawer.querySelectorAll<HTMLInputElement>('[data-test=target-configs] input')[1]!
    c2.checked = true
    c2.dispatchEvent(new Event('change'))
    ;(drawer.querySelector('[data-test=target-save]') as HTMLButtonElement).click()
    await flushPromises()
    const put = calls.find((c) => c.init.method === 'PUT')!
    expect(JSON.parse(String(put.init.body))).toEqual({ name: 'Edge servers', auto_deploy: true, certificate_filters: [{ common_name: 'web-.*' }] })
    const attach = calls.find((c) => c.init.method === 'POST' && c.url.endsWith('/targets/t1/configurations'))!
    expect(JSON.parse(String(attach.init.body))).toMatchObject({ configuration_ids: ['c2'] })
    w.unmount()
  })

  it('configurations: credentials are write-only and never echoed in refusal text', async () => {
    const calls = fetchMock((url, init) => (init.method === 'POST' && url.endsWith('/configurations') ? new Response(JSON.stringify({ reason: 'validation_failed', detail: { fields: { config: 'host is required' } } }), { status: 422 }) : url.includes('/providers') ? { items: [provider] } : { items: [config] }))
    const w = mount(Configurations, { global, attachTo: document.body })
    await flushPromises()
    await w.find('[data-test=config-new]').trigger('click')
    await flushPromises()
    const drawer = document.body.querySelector('aside[role=dialog]')!
    const name = drawer.querySelector<HTMLInputElement>('input[data-field=name]')!
    name.value = 'New'
    name.dispatchEvent(new Event('input'))
    const prov = drawer.querySelector<HTMLSelectElement>('select[data-field=provider_type]')!
    prov.value = 'ssh'
    prov.dispatchEvent(new Event('change'))
    const creds = drawer.querySelector<HTMLTextAreaElement>('textarea[data-field=credentials]')!
    creds.value = '{"private_key":"SECRET-KEY-MATERIAL"}'
    creds.dispatchEvent(new Event('input'))
    ;(drawer.querySelector('[data-test=config-save]') as HTMLButtonElement).click()
    await flushPromises()
    const post = calls.find((c) => c.init.method === 'POST')!
    expect(JSON.parse(String(post.init.body))).toEqual({ name: 'New', provider_type: 'ssh', config: {}, credentials: { private_key: 'SECRET-KEY-MATERIAL' } })
    // The server refusal maps onto the config field; the page never prints the secret.
    expect(drawer.querySelector('[role=alert]')?.textContent).toContain('host is required')
    expect(drawer.textContent).not.toContain('SECRET-KEY-MATERIAL')
    expect(w.find('[style]').exists()).toBe(false)
    w.unmount()
  })

  it('jobs: table with progress and detail drawer with history', async () => {
    fetchMock((url) => (url.includes('/result') ? { id: 'j1', certificate_id: 'cert-1', status: 'failed', type: 'direct', progress: 50, retry_count: 1, max_retries: 3, triggered_by: 'manual', created_at: '2026-01-01T00:00:00Z', history: [{ action: 'deploy', result: 'failure', duration_ms: 120, created_at: '2026-01-01T00:00:00Z', message: 'ssh timeout' }] } : { items: [{ id: 'j1', certificate_id: 'cert-1', status: 'failed', type: 'direct', progress: 50, retry_count: 1, max_retries: 3, triggered_by: 'manual', created_at: '2026-01-01T00:00:00Z' }] }))
    const w = mount(Jobs, { global, attachTo: document.body })
    await flushPromises()
    expect(w.find('[data-test="job-row-j1"] progress').attributes('value')).toBe('50')
    await w.find('[data-test="job-row-j1"]').trigger('click')
    await flushPromises()
    const drawer = document.body.querySelector('aside[role=dialog]')!
    expect(drawer.textContent).toContain('ssh timeout')
    expect(Array.from(drawer.querySelectorAll('button')).map((b) => b.textContent?.trim())).toEqual(expect.arrayContaining(['Retry', 'Verify', 'Rollback']))
    w.unmount()
  })
})
