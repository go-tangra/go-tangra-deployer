// Jobs on the list contract (specs 032): server paging and sorting, the URL
// state, server-paged detail tables, and live events that keep the page.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount } from '@vue/test-utils'
import { createRouter, createMemoryHistory } from 'vue-router'
import Jobs from '@/views/jobs/index.vue'
import Dashboard from '@/views/dashboard/index.vue'
import { useLive } from '@/stores/live'
import { LIVE_RELOAD_MS } from '@/stores/jobs'

type Call = { url: string; init: RequestInit }
function fetchMock(handler: (url: string, init: RequestInit) => unknown) {
  const calls: Call[] = []
  vi.stubGlobal('fetch', vi.fn(async (url: string, init: RequestInit = {}) => {
    calls.push({ url, init })
    const body = handler(url, init)
    if (body instanceof Response) return body
    return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
  }))
  return calls
}
class FakeSource { onopen = null; onerror = null; addEventListener() {} close() {} }
const params = (url: string) => new URL(url, 'https://x').searchParams
const job = (id: string, over: Record<string, unknown> = {}) => ({ id, certificate_id: 'cert-' + id, status: 'processing', type: 'direct', progress: 10, retry_count: 0, max_retries: 3, triggered_by: 'manual', created_at: '2026-01-01T00:00:00Z', ...over })

// A server of `total` jobs that echoes the request and clamps the page.
function server(total = 130, extra: (url: string) => unknown = () => undefined) {
  return (url: string) => {
    const e = extra(url)
    if (e !== undefined) return e
    const q = params(url)
    const size = Number(q.get('page_size'))
    const page = Math.min(Number(q.get('page')), Math.max(1, Math.ceil(total / size)))
    return { items: [job('p' + page)], total, page, page_size: size, sort: q.get('sort'), order: q.get('order') }
  }
}
const lists = (calls: Call[]) => calls.filter((c) => c.url.startsWith('/api/deployer/v1/jobs?'))
const header = (w: ReturnType<typeof mount>, label: string) => w.findAll('th button').find((b) => b.text().startsWith(label))
function mkRouter() {
  return createRouter({ history: createMemoryHistory(), routes: [{ path: '/deployer/jobs', component: Jobs }] })
}

describe('jobs: server paging, sorting and live events', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    vi.stubGlobal('EventSource', FakeSource)
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
  })
  afterEach(() => vi.useRealTimers())

  it('pager with the total, whole-list sort on server fields, filters back to page 1 keeping the sort', async () => {
    const calls = fetchMock(server())
    const r = mkRouter()
    await r.push('/deployer/jobs')
    const w = mount(Jobs, { global: { plugins: [r] }, attachTo: document.body })
    await flushPromises()
    const last = () => params(lists(calls).at(-1)!.url)
    expect(lists(calls)[0]!.url).toBe('/api/deployer/v1/jobs?page=1&page_size=25&sort=created_at&order=desc')
    expect(w.text()).toContain('Showing 1–25 of 130')
    await w.find('[aria-label="Page 3"]').trigger('click')
    await flushPromises()
    expect(last().get('page')).toBe('3')
    expect(r.currentRoute.value.query['jobs.page']).toBe('3')
    for (const label of ['Type', 'Status', 'Created', 'Completed']) expect(header(w, label), label).toBeTruthy()
    expect(header(w, 'Certificate')).toBeUndefined()
    await header(w, 'Status')!.trigger('click')
    await flushPromises()
    expect([last().get('sort'), last().get('page')]).toEqual(['status', '1'])
    await header(w, 'Type')!.trigger('click')
    await flushPromises()
    expect(last().get('sort')).toBe('job_type')
    // The refresh button keeps the page.
    await w.find('[aria-label="Page 2"]').trigger('click')
    await flushPromises()
    await w.find('[data-test=jobs-refresh]').trigger('click')
    await flushPromises()
    expect([last().get('page'), last().get('sort')]).toEqual(['2', 'job_type'])
    // A filter change returns to page 1 and keeps the sort.
    await w.findAll('select')[0]!.setValue('failed')
    await flushPromises()
    expect([last().get('status'), last().get('page'), last().get('sort')]).toEqual(['failed', '1', 'job_type'])
    w.unmount()
  })

  it('URL state is read on load; the server-clamped page is adopted', async () => {
    const calls = fetchMock(server(31))
    const r = mkRouter()
    await r.push('/deployer/jobs?jobs.page=9&jobs.size=10&jobs.sort=completed_at&jobs.order=asc')
    const w = mount(Jobs, { global: { plugins: [r] }, attachTo: document.body })
    await flushPromises()
    const first = params(lists(calls)[0]!.url)
    expect([first.get('page'), first.get('page_size'), first.get('sort'), first.get('order')]).toEqual(['9', '10', 'completed_at', 'asc'])
    expect(r.currentRoute.value.query['jobs.page']).toBe('4')
    w.unmount()
  })

  it('a live event for a job on the page patches it in place; any other job reloads the current page (debounced)', async () => {
    const calls = fetchMock(server())
    const r = mkRouter()
    await r.push('/deployer/jobs?jobs.page=3&jobs.sort=status&jobs.order=asc')
    const w = mount(Jobs, { global: { plugins: [r] }, attachTo: document.body })
    await flushPromises()
    await w.findAll('select')[1]!.setValue('direct') // a filter: back to page 1
    await flushPromises()
    await w.find('[aria-label="Page 3"]').trigger('click')
    await flushPromises()
    const before = calls.length
    vi.useFakeTimers()
    const live = useLive()
    live._emit('job.updated', JSON.stringify({ job_id: 'p3', status: 'completed', progress: 100 }))
    await flushPromises()
    expect(w.find('[data-test="job-row-p3"] progress').attributes('value')).toBe('100')
    expect(w.find('[data-test="job-row-p3"]').text()).toContain('completed')
    vi.advanceTimersByTime(LIVE_RELOAD_MS * 2)
    expect(calls.length).toBe(before) // no reload for a visible job
    // Unknown jobs: a burst of events reloads once, on the same page, order and filter.
    live._emit('deployment.completed', JSON.stringify({ job_id: 'new-1', status: 'completed', progress: 100 }))
    live._emit('deployment.failed', JSON.stringify({ job_id: 'new-2', status: 'failed', progress: 100 }))
    expect(calls.length).toBe(before)
    vi.advanceTimersByTime(LIVE_RELOAD_MS)
    vi.useRealTimers()
    await flushPromises()
    expect(calls.length).toBe(before + 1)
    const q = params(calls.at(-1)!.url)
    expect([q.get('page'), q.get('sort'), q.get('order'), q.get('job_type')]).toEqual(['3', 'status', 'asc', 'direct'])
    expect(r.currentRoute.value.query['jobs.page']).toBe('3')
    w.unmount()
  })

  it('detail drawer: child jobs and history are server-paged and restart at page 1 for another job', async () => {
    const parent = job('par', { type: 'parent', status: 'partial' })
    const calls = fetchMock(server(2, (url) => {
      if (url.includes('/children?')) {
        const q = params(url)
        return { items: [job('c' + q.get('page'), { type: 'child', target_configuration_id: 'cfg-' + q.get('page') })], total: 23, page: Number(q.get('page')), page_size: 10, sort: 'created_at', order: q.get('order') }
      }
      if (url.includes('/history?')) return { items: [{ id: 'h1', action: 'deploy', result: 'failure', duration_ms: 5, message: 'boom', created_at: '2026-01-01T00:00:00Z' }], total: 1, page: 1, page_size: 10, sort: 'created_at', order: 'desc' }
      if (url.startsWith('/api/deployer/v1/jobs?')) return { items: [parent, job('d1')], total: 2, page: 1, page_size: 25, sort: 'created_at', order: 'desc' }
      return undefined
    }))
    const r = mkRouter()
    await r.push('/deployer/jobs')
    const w = mount(Jobs, { global: { plugins: [r] }, attachTo: document.body })
    await flushPromises()
    await w.find('[data-test="job-row-par"]').trigger('click')
    await flushPromises()
    const drawer = () => document.body.querySelector('aside[role=dialog]')!
    const kids = () => calls.filter((c) => c.url.includes('/jobs/par/children?'))
    expect(params(kids()[0]!.url).get('page')).toBe('1')
    expect(drawer().querySelector('[data-test=job-children]')!.textContent).toContain('cfg-1')
    expect(drawer().querySelector('[data-test=job-history]')!.textContent).toContain('boom')
    ;(drawer().querySelector('[data-test=job-children] [aria-label="Page 3"]') as HTMLButtonElement).click()
    await flushPromises()
    expect(params(kids().at(-1)!.url).get('page')).toBe('3')
    expect(drawer().querySelector('[data-test=job-children]')!.textContent).toContain('cfg-3')
    // A direct job has no children request; its history starts at page 1.
    await w.find('[data-test="job-row-d1"]').trigger('click')
    await flushPromises()
    expect(calls.some((c) => c.url.includes('/jobs/d1/children'))).toBe(false)
    expect(params(calls.filter((c) => c.url.includes('/jobs/d1/history?')).at(-1)!.url).get('page')).toBe('1')
    expect(r.currentRoute.value.query['children.page']).toBeUndefined()
    w.unmount()
  })

  it('dashboard falls back to list totals on its own first pages when statistics are unavailable', async () => {
    const calls = fetchMock((url) =>
      url.includes('/statistics')
        ? new Response(JSON.stringify({ reason: 'temporarily_unavailable' }), { status: 503, headers: { 'Content-Type': 'application/json' } })
        : url.includes('/targets?') ? { items: [], total: 41, page: 1, page_size: 25, sort: 'name', order: 'asc' }
        : url.includes('/configurations?') ? { items: [], total: 17, page: 1, page_size: 25, sort: 'name', order: 'asc' }
        : { items: [job('a', { status: 'completed' })], total: 900, page: 1, page_size: 25, sort: 'created_at', order: 'desc' },
    )
    const w = mount(Dashboard, { global: { plugins: [mkRouter()] } })
    await flushPromises()
    expect(w.text()).toContain('41')
    expect(w.text()).toContain('17')
    const j = params(calls.find((c) => c.url.includes('/jobs?'))!.url)
    expect([j.get('page'), j.get('sort'), j.get('order')]).toEqual(['1', 'created_at', 'desc'])
    w.unmount()
  })
})
