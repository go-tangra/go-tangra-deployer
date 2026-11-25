// Target status e-mails: the "Send notification" switch and, when it is on,
// "Notify on" (every finished deployment / only failures) are saved with the
// target; ?job=<id> (the e-mail's link) opens that job on the jobs page.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import Targets from '@/views/targets/index.vue'
import Jobs from '@/views/jobs/index.vue'
import { body, catalogue, choose, click, FakeSource, fetchMock, mkRouter, page, type, type Call, type Handler } from './helpers'

const stored = { id: 't1', name: 'edge', auto_deploy: false, notify_enabled: true, notify_on: 'failure', certificate_filters: [], configuration_ids: [] }

function api(): Handler {
  return (url, init) => {
    const u = new URL(url, 'https://x')
    if (u.pathname.endsWith('/providers')) return catalogue
    if (u.pathname.endsWith('/targets') && init.method === 'POST') return { ...stored, id: 't2' }
    if (u.pathname.endsWith('/targets/t1') && init.method === 'PUT') return stored
    if (u.pathname.endsWith('/targets/t1')) return stored
    if (u.pathname.endsWith('/targets')) return page([stored])
    return page([])
  }
}

let w: VueWrapper | null = null
let calls: Call[] = []
const q = <T extends Element = HTMLElement>(sel: string) => document.body.querySelector<T>(sel)
const toggle = () => q<HTMLInputElement>('[data-test=target-notify] input, input[data-test=target-notify]')
const notifyOn = () => q<HTMLSelectElement>('[data-test=target-notify-on] select, select[data-test=target-notify-on]')

describe('target status e-mails', () => {
  beforeEach(async () => {
    setActivePinia(createPinia())
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    vi.stubGlobal('EventSource', FakeSource)
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
    calls = fetchMock(api())
  })
  afterEach(() => {
    w?.unmount()
    w = null
  })

  it('a new target sends no notification until switched on, then saves when to send', async () => {
    w = mount(Targets, { global: { plugins: [mkRouter()] }, attachTo: document.body })
    await flushPromises()
    await click(q('[data-test=target-new]'))
    await type(q<HTMLInputElement>('input[data-field=name]'), 'web')
    expect(toggle()!.checked).toBe(false)
    expect(notifyOn()).toBeNull() // only shown when notifications are on
    expect(document.body.textContent).toContain('owners, admins and deployer administrators')
    toggle()!.checked = true
    toggle()!.dispatchEvent(new Event('change'))
    await flushPromises()
    expect(notifyOn()!.value).toBe('always')
    await choose(notifyOn(), 'failure')
    await click(q('[data-test=target-save]'))
    expect(body(calls, 'POST', '/targets')).toMatchObject({ name: 'web', notify_enabled: true, notify_on: 'failure' })
  })

  it('editing a target shows and keeps its notification settings', async () => {
    w = mount(Targets, { global: { plugins: [mkRouter()] }, attachTo: document.body })
    await flushPromises()
    await click(q('[data-test=target-row-t1]'))
    expect(toggle()!.checked).toBe(true)
    expect(notifyOn()!.value).toBe('failure')
    toggle()!.checked = false
    toggle()!.dispatchEvent(new Event('change'))
    await flushPromises()
    expect(notifyOn()).toBeNull()
    await click(q('[data-test=target-save]'))
    expect(body(calls, 'PUT', '/targets/t1')).toMatchObject({ notify_enabled: false, notify_on: 'failure' })
  })

  it('the jobs page opens the job named by ?job=', async () => {
    const job = { id: 'p1', type: 'parent', certificate_id: 'c', status: 'partial', progress: 50, retry_count: 0, max_retries: 3, triggered_by: 'event', created_at: '2026-01-01T00:00:00Z', history: [] }
    calls = fetchMock((url) => (url.includes('/jobs/p1/result') ? job : page([])))
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/deployer/jobs', component: { template: '<div/>' } }] })
    await router.push('/deployer/jobs?job=p1')
    w = mount(Jobs, { global: { plugins: [router] }, attachTo: document.body })
    await flushPromises()
    expect(calls.some((c) => c.url.includes('/jobs/p1/result'))).toBe(true)
    expect(q('aside[role=dialog]')).not.toBeNull()
  })
})
