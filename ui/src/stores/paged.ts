import { ref, type Ref, type UnwrapRef } from 'vue'
import type { ListParams } from '@go-tangra/ui'
import { api, describe } from '@/api/client'

/** One server page of a list endpoint (go-tangra list contract, specs 032). */
export interface Page<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  sort: string
  order: 'asc' | 'desc'
}

export type ListFilter = Record<string, string | undefined>

/**
 * Server-paged list state for a store: the current page's rows, the total,
 * the last page parameters and filter. A response superseded by a newer
 * request is ignored (rapid paging / sorting).
 */
export function pagedList<T, F extends ListFilter>(path: string, first: ListParams) {
  // UnwrapRef (as ref<T[]>() gives) keeps rows assignable to the kit table's row type.
  const items = ref([]) as Ref<UnwrapRef<T>[]>
  const total = ref(0)
  const params = ref<ListParams>({ ...first })
  const filter = ref({}) as Ref<F>
  const loading = ref(false)
  const error = ref('')
  let seq = 0

  /** Loads one page; resolves with it, or null when it failed or was superseded. */
  async function list(f: F = filter.value, q: ListParams = params.value): Promise<Page<T> | null> {
    const mine = ++seq
    loading.value = true
    error.value = ''
    filter.value = { ...f }
    params.value = { ...q }
    try {
      const res = await api<Page<T>>('GET', path, undefined, { query: { ...f, ...q } })
      if (mine !== seq) return null
      items.value = (res.items ?? []) as UnwrapRef<T>[]
      total.value = res.total ?? 0
      return res
    } catch (e) {
      if (mine === seq) error.value = describe(e)
      return null
    } finally {
      if (mine === seq) loading.value = false
    }
  }

  /** Reloads the current page with the current filter and order. */
  const reload = () => list()

  return { items, total, params, filter, loading, error, list, reload }
}

/** Every row of a list endpoint (all pages of the maximum size), e.g. for pickers. */
export async function fetchAll<T>(path: string, sort: string, order: 'asc' | 'desc'): Promise<T[]> {
  const out: T[] = []
  for (let page = 1; ; page++) {
    const res = await api<Page<T>>('GET', path, undefined, { query: { page, page_size: 200, sort, order } })
    out.push(...(res.items ?? []))
    if (!res.items?.length || out.length >= (res.total ?? 0)) return out
  }
}
