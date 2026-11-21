// Target row actions: Deploy (a certificate picked by host name, deployed to
// every configuration attached to the target) and Delete.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import Targets from '@/views/targets/index.vue'
import { answer, body, catalogue, click, FakeSource, fetchMock, mkRouter, page, type, type Call, type Handler } from './helpers'

const global = { plugins: [mkRouter()] }
const PBX = '01a1c472-ea51-7485-8344-9390d39bb85a'
const cert = { id: PBX, subject: 'pbx.example.org', sans: ['pbx.example.org'], serial: '51190', issuer_id: 'i1', status: 'active', not_after: '2027-02-08T06:55:24Z', has_key: true }
const targets = [
  { id: 't1', name: 'edge', auto_deploy: false, certificate_filters: [], configuration_ids: ['c1', 'c2'] },
  { id: 't2', name: 'empty', auto_deploy: false, certificate_filters: [], configuration_ids: [] },
]

function api(): Handler {
  return (url, init) => {
    const u = new URL(url, 'https://x')
    if (u.pathname.endsWith('/providers')) return catalogue
    if (u.pathname.endsWith('/certificates')) return { items: u.searchParams.get('name') === 'pbx.example.org' ? [cert] : [] }
    if (u.pathname.endsWith('/deploy/target') && init.method === 'POST') return { status: 'processing', job_id: 'j1' }
    if (u.pathname.endsWith('/targets/t1/remove') && init.method === 'POST') return new Response(null, { status: 204 })
    if (u.pathname.endsWith('/targets')) return page(targets)
    return page([])
  }
}

let w: VueWrapper | null = null
let calls: Call[] = []
const q = <T extends Element = HTMLElement>(sel: string) => document.body.querySelector<T>(sel)

describe('target row actions', () => {
  beforeEach(async () => {
    setActivePinia(createPinia())
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    vi.stubGlobal('EventSource', FakeSource)
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
    calls = fetchMock(api())
    w = mount(Targets, { global, attachTo: document.body })
    await flushPromises()
  })
  afterEach(() => {
    vi.useRealTimers()
    w?.unmount()
    w = null
  })

  it('deploys a picked certificate to the target', async () => {
    expect((q<HTMLButtonElement>('[data-test=target-deploy-t2]')!).disabled).toBe(true) // nothing attached
    vi.useFakeTimers()
    await click(q('[data-test=target-deploy-t1]'))
    expect(q('[data-test=target-name]')).toBeNull() // the row click does not open the drawer
    expect(document.body.textContent).toContain('Every configuration attached to this target (2)')
    await click(q('[data-test=target-deploy-submit]'))
    expect(body(calls, 'POST', '/deploy/target')).toBeUndefined() // nothing picked
    await type(q<HTMLInputElement>('[data-test=deploy-hostname] input, input[data-test=deploy-hostname]'), 'pbx.example.org')
    await vi.advanceTimersByTimeAsync(300)
    await flushPromises()
    await click(q('[data-test=target-deploy-submit]'))
    expect(body(calls, 'POST', '/deploy/target')).toEqual({ certificate_id: PBX, target_id: 't1' })
  })

  it('deletes a target from its row after confirmation', async () => {
    await click(q('[data-test=target-delete-t1]'))
    answer(false)
    await flushPromises()
    expect(calls.some((c) => c.url.endsWith('/targets/t1/remove'))).toBe(false)
    await click(q('[data-test=target-delete-t1]'))
    answer(true)
    await flushPromises()
    expect(calls.some((c) => c.url.endsWith('/targets/t1/remove') && c.init.method === 'POST')).toBe(true)
  })
})
