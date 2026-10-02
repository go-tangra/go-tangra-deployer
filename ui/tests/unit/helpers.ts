// Shared fixtures of the deployer UI unit tests: a fetch mock that records
// calls, the provider catalogue (the backend golden file — the UI renders what
// the API serves), a router and axe options for jsdom.
import { vi } from 'vitest'
import { flushPromises } from '@vue/test-utils'
import { createRouter, createMemoryHistory } from 'vue-router'
import { axe } from 'vitest-axe'
import { useConfirm } from '@go-tangra/ui'
import goldenJSON from '../../../internal/providers/all/testdata/capabilities.golden.json'
import type { Provider } from '@/api/types'

export const catalogue = goldenJSON as unknown as { items: Provider[] }
export const provider = (type: string) => catalogue.items.find((p) => p.type === type)!

export type Call = { url: string; init: RequestInit }
export type Handler = (url: string, init: RequestInit) => unknown

/** Responds with handler's value: a Response as is, 204, or JSON 200. */
export function fetchMock(handler: Handler): Call[] {
  const calls: Call[] = []
  vi.stubGlobal('fetch', vi.fn(async (url: string, init: RequestInit = {}) => {
    calls.push({ url, init })
    const body = handler(url, init)
    if (body === 204) return new Response(null, { status: 204 })
    if (body instanceof Response) return body
    return new Response(JSON.stringify(body ?? {}), { status: 200, headers: { 'Content-Type': 'application/json' } })
  }))
  return calls
}

/** A JSON refusal. */
export const refuse = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })

export const page = <T>(items: T[]) => ({ items, total: items.length, page: 1, page_size: 25, sort: 'name', order: 'asc' })

export class FakeSource {
  onopen = null
  onerror = null
  addEventListener() {}
  close() {}
}

export const mkRouter = () => createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div/>' } }] })

export const drawer = () => document.body.querySelector<HTMLElement>('aside[role=dialog]')!
export const q = <T extends Element = HTMLElement>(sel: string) => drawer().querySelector<T>(sel)

export async function type(el: HTMLInputElement | HTMLTextAreaElement | null, value: string): Promise<void> {
  if (!el) throw new Error('no input')
  el.value = value
  el.dispatchEvent(new Event('input'))
  await flushPromises()
}

export async function choose(el: HTMLSelectElement | null, value: string): Promise<void> {
  if (!el) throw new Error('no select')
  el.value = value
  el.dispatchEvent(new Event('change'))
  await flushPromises()
}

export async function click(el: Element | null): Promise<void> {
  if (!el) throw new Error('no element')
  ;(el as HTMLElement).click()
  await flushPromises()
}

/** The JSON body of the last call matching method + path suffix. */
export function body(calls: Call[], method: string, suffix: string): Record<string, unknown> | undefined {
  const c = [...calls].reverse().find((x) => x.init.method === method && x.url.split('?')[0]!.endsWith(suffix))
  return c ? (JSON.parse(String(c.init.body)) as Record<string, unknown>) : undefined
}

/** Answers the pending confirm dialog (the shell hosts UiConfirm). */
export function answer(ok: boolean): void {
  useConfirm().answer(ok)
}

export async function axeViolations(el: Element): Promise<string[]> {
  const res = await axe(el as HTMLElement, { rules: { 'color-contrast': { enabled: false }, region: { enabled: false } } })
  // The kit drawer is an <aside role="dialog"> (kit 4.3): axe's aria-allowed-role
  // flags that element itself; it is the kit's and out of this module's reach.
  return res.violations
    .map((v) => ({ id: v.id, nodes: v.nodes.filter((n) => !(v.id === 'aria-allowed-role' && /^aside/.test(String(n.target[0])))) }))
    .filter((v) => v.nodes.length)
    .map((v) => v.id + ': ' + v.nodes.map((n) => n.target.join(' ')).join(', '))
}
