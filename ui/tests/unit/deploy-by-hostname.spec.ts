// The Deploy dialog takes a host name, not a certificate id: lcm's live
// certificates for it are listed (GET /certificates?name=) and the picked
// one's id is deployed.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import Configurations from '@/views/configurations/index.vue'
import { axeViolations, body, catalogue, click, FakeSource, fetchMock, mkRouter, page, refuse, type, type Call, type Handler } from './helpers'

const global = { plugins: [mkRouter()] }
const PBX = '01a1c472-ea51-7485-8344-9390d39bb85a'
const PBX2 = '01a1c472-ea51-7485-8344-000000000002'
const WILD = '01a1c472-ea51-7485-8344-000000000003'
const cert = (id: string, subject: string, extra: Record<string, unknown> = {}) => ({ id, subject, sans: [subject], serial: '5119' + id.slice(-1), issuer_id: 'i1', status: 'active', not_after: '2027-02-08T06:55:24Z', has_key: true, ...extra })
const row = { id: 'c1', name: 'pbx files', provider_type: 'dummy', status: 'active', has_credentials: false, config: {}, target_supplied: [] }

function api(): Handler {
  return (url, init) => {
    const u = new URL(url, 'https://x')
    if (u.pathname.endsWith('/providers')) return catalogue
    if (u.pathname.endsWith('/certificates')) {
      const name = u.searchParams.get('name') ?? ''
      if (name.includes(' ')) return refuse(422, { reason: 'validation_failed', detail: { param: 'name' } })
      if (name === 'pbx') return { items: [cert(PBX, 'pbx.example.org'), cert(PBX2, 'pbx2.example.org', { has_key: false, status: 'expiring' })] }
      if (name === 'pbx.example.org') return { items: [cert(PBX, 'pbx.example.org'), cert(WILD, '*.example.org', { sans: ['*.example.org', 'example.org'] })] }
      if (name === 'web.example.org') return { items: [cert(WILD, '*.example.org', { sans: ['*.example.org', 'example.org'] })] }
      return { items: [] }
    }
    if (u.pathname.endsWith('/deploy') && init.method === 'POST') return { status: 'processing', job_id: 'j1' }
    if (u.pathname.endsWith('/configurations')) return page([row])
    return page([])
  }
}

let w: VueWrapper | null = null
let calls: Call[] = []
const q = <T extends Element = HTMLElement>(sel: string) => document.body.querySelector<T>(sel)
const hostInput = () => q<HTMLInputElement>('[data-test=deploy-hostname] input, input[data-test=deploy-hostname]')
const option = (id: string) => q<HTMLInputElement>(`[data-test="certificate-option-${id}"] input`)
const lookups = () => calls.filter((c) => c.url.includes('/certificates?')).map((c) => new URL(c.url, 'https://x').searchParams.get('name'))
async function search(name: string): Promise<void> {
  await type(hostInput(), name)
  await vi.advanceTimersByTimeAsync(300)
  await flushPromises()
}

describe('deploy by host name', () => {
  beforeEach(async () => {
    setActivePinia(createPinia())
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    vi.stubGlobal('EventSource', FakeSource)
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
    calls = fetchMock(api())
    w = mount(Configurations, { global, attachTo: document.body })
    await flushPromises()
    vi.useFakeTimers()
    await w.find('[data-test=config-deploy-c1]').trigger('click')
    await flushPromises()
  })
  afterEach(() => {
    vi.useRealTimers()
    w?.unmount()
    w = null
  })

  it('lists the matches; nothing deploys until one is picked; the picked id is deployed', async () => {
    expect(hostInput()).not.toBeNull()
    await search('pbx')
    expect(lookups()).toEqual(['pbx'])
    expect(option(PBX)!.checked).toBe(false) // no exact match: the operator picks
    expect(option(PBX2)!.checked).toBe(false)
    expect(q(`[data-test="certificate-option-${PBX2}"]`)!.textContent).toContain('no stored key')
    expect(q(`[data-test="certificate-option-${PBX2}"]`)!.textContent).toContain('expiring')
    await click(q('[data-test=deploy-submit]'))
    expect(body(calls, 'POST', '/deploy')).toBeUndefined()
    expect(document.body.textContent).toContain('Type a host name and pick a certificate.')
    await click(option(PBX))
    await click(q('[data-test=deploy-submit]'))
    expect(body(calls, 'POST', '/deploy')).toEqual({ certificate_id: PBX, configuration_id: 'c1' })
  })

  it('an exact name, or the only match (a wildcard), is preselected', async () => {
    await search('pbx.example.org')
    expect(option(PBX)!.checked).toBe(true)
    expect(option(WILD)!.checked).toBe(false)
    expect(q(`[data-test="certificate-option-${WILD}"]`)!.textContent).toContain('example.org')
    await search('web.example.org')
    expect(option(WILD)!.checked).toBe(true)
    await click(q('[data-test=deploy-submit]'))
    expect(body(calls, 'POST', '/deploy')).toEqual({ certificate_id: WILD, configuration_id: 'c1' })
  })

  it('no match and a refused name are said; clearing the name clears the pick', async () => {
    await search('nothing.example.org')
    expect(q('[data-test=certificate-none]')!.textContent).toContain('nothing.example.org')
    await search('a b')
    expect(q('[data-test=certificate-error]')!.textContent).toContain('Enter a host name')
    await search('pbx.example.org')
    expect(option(PBX)!.checked).toBe(true)
    await search('')
    expect(option(PBX)).toBeNull()
    await click(q('[data-test=deploy-submit]'))
    expect(body(calls, 'POST', '/deploy')).toBeUndefined()
  })

  it('has no axe violations with results listed', async () => {
    await search('pbx')
    vi.useRealTimers()
    expect(await axeViolations(document.querySelector('[data-test=certificate-picker]')!)).toEqual([])
  })
})
