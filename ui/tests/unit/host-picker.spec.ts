// T096: the host picker: server-paged search over the inventory host list
// (user's session), chips, agent badges, manual fallback without access.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { defineComponent, h, ref } from 'vue'
import HostPicker from '@/components/HostPicker.vue'
import { axeViolations, fetchMock, refuse, type Call, type Handler } from './helpers'

const host = (n: number) => ({ id: `0192a7c0-0000-7000-8000-${String(n).padStart(12, '0')}`, hostname: `web-${n}`, os_name: 'Debian', os_version: '12', tags: { role: 'web' } })
const all = Array.from({ length: 23 }, (_, i) => host(i + 1))

function inventory(extra: Handler = () => undefined): Handler {
  return (url, init) => {
    const e = extra(url, init)
    if (e !== undefined) return e
    const u = new URL(url, 'https://x')
    if (u.pathname === '/api/inventory/v1/hosts') {
      const name = u.searchParams.get('hostname') ?? ''
      const size = Number(u.searchParams.get('page_size'))
      const pg = Number(u.searchParams.get('page'))
      const hits = all.filter((x) => x.hostname.includes(name))
      return { items: hits.slice((pg - 1) * size, pg * size), total: hits.length, page: pg, page_size: size, sort: 'hostname', order: 'asc' }
    }
    if (u.pathname === '/api/inventory/v1/agents') return { items: [{ host_id: all[0]!.id, online: true, certificate_capability: 'enabled' }, { host_id: all[1]!.id, online: false, certificate_capability: 'upgrade_required' }], total: 2, page: 1, page_size: 200 }
    return refuse(404, { reason: 'not_found' })
  }
}

// The picker inside a v-model owner, as the form uses it.
const Owner = defineComponent({
  setup() {
    const ids = ref<string[]>([])
    return { ids }
  },
  render() {
    return h('div', [h(HostPicker, { id: 'config.host_ids', label: 'Hosts', modelValue: this.ids, max: 3, 'onUpdate:modelValue': (v: string[]) => (this.ids = v) })])
  },
})

let w: VueWrapper | null = null
async function mountPicker(handler: Handler): Promise<Call[]> {
  const calls = fetchMock(handler)
  w = mount(Owner, { attachTo: document.body })
  await flushPromises()
  return calls
}
const hostCalls = (calls: Call[]) => calls.filter((c) => c.url.startsWith('/api/inventory/v1/hosts?')).map((c) => new URL(c.url, 'https://x').searchParams)

describe('host picker', () => {
  beforeEach(() => {
    document.cookie = '__Host-csrf=tok; Secure; Path=/'
    ;(globalThis as unknown as { __vw: number }).__vw = 1280
  })
  afterEach(() => {
    w?.unmount()
    w = null
    vi.useRealTimers()
  })

  it('server-paged, searchable host table with agent and capability badges; selection as chips (max items)', async () => {
    const calls = await mountPicker(inventory())
    const first = hostCalls(calls)[0]!
    expect([first.get('page'), first.get('page_size'), first.get('sort')]).toEqual(['1', '10', 'hostname'])
    expect(w!.text()).toContain('Showing 1–10 of 23')
    const row1 = w!.find(`[data-test="host-row-${all[0]!.id}"]`)
    expect(row1.text()).toContain('online')
    expect(row1.text()).toContain('Certificates')
    expect(w!.find(`[data-test="host-row-${all[1]!.id}"]`).text()).toContain('Agent upgrade required')
    expect(w!.find(`[data-test="host-row-${all[2]!.id}"]`).text()).toContain('no agent')
    await w!.find('[aria-label="Page 3"]').trigger('click')
    await flushPromises()
    expect(hostCalls(calls).at(-1)!.get('page')).toBe('3')
    // Search by hostname: back to page 1 (debounced).
    vi.useFakeTimers()
    await w!.find('input[type=search]').setValue('web-2')
    vi.advanceTimersByTime(300)
    vi.useRealTimers()
    await flushPromises()
    const last = hostCalls(calls).at(-1)!
    expect([last.get('hostname'), last.get('page')]).toEqual(['web-2', '1'])
    for (const n of [2, 20, 21, 22]) {
      const box = w!.find(`[data-test="host-row-${host(n).id}"] input[type=checkbox]`)
      await box.setValue(true)
    }
    const owner = w!.vm as unknown as { ids: string[] }
    expect(owner.ids).toEqual([host(2).id, host(20).id, host(21).id]) // max 3
    expect(w!.find('[data-test=host-chips]').text()).toContain('web-20')
    await w!.find('[aria-label="Remove web-20"]').trigger('click')
    expect(owner.ids).toEqual([host(2).id, host(21).id])
    expect(await axeViolations(w!.element)).toEqual([])
  })

  for (const status of [401, 403, 404]) {
    it(`${status} on the host list → manual entry notice and id text area`, async () => {
      await mountPicker(inventory((url) => (url.startsWith('/api/inventory/v1/hosts') ? refuse(status, { reason: 'forbidden' }) : undefined)))
      expect(w!.find('[data-test=host-picker-manual]').text()).toContain('enter host ids manually')
      const ta = w!.find('textarea')
      await ta.setValue(`${host(1).id}\n${host(2).id}, ${host(1).id}`)
      expect((w!.vm as unknown as { ids: string[] }).ids).toEqual([host(1).id, host(2).id])
    })
  }

  it('without agent access the picker still lists and selects hosts', async () => {
    await mountPicker(inventory((url) => (url.startsWith('/api/inventory/v1/agents') ? refuse(403, { reason: 'forbidden' }) : undefined)))
    expect(w!.find('[data-test=host-picker-manual]').exists()).toBe(false)
    expect(w!.find(`[data-test="host-row-${all[0]!.id}"]`).text()).toContain('web-1')
  })
})
