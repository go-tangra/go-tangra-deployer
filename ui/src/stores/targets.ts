import { defineStore } from 'pinia'
import type { ListParams, ListQueryOptions } from '@go-tangra/ui'
import { api } from '@/api/client'
import type { Target, TargetInput } from '@/api/types'
import { pagedList } from '@/stores/paged'

/** Sortable fields of GET /targets (server Spec store.TargetList). */
export const TARGET_SORTS = ['name', 'created_at'] as const
export const TARGET_LIST: ListQueryOptions = { sortable: [...TARGET_SORTS], defaultSort: { key: 'name', dir: 'asc' }, defaultSize: 25 }
const FIRST_PAGE: ListParams = { page: 1, page_size: 25, sort: 'name', order: 'asc' }

export const useTargets = defineStore('deployer-targets', () => {
  const paged = pagedList<Target, Record<string, string | undefined>>('targets', FIRST_PAGE)

  /** A single target with its overrides and per-configuration missing_required. */
  async function get(id: string): Promise<Target> {
    return api<Target>('GET', 'targets/' + id)
  }

  async function create(input: TargetInput): Promise<Target> {
    return api<Target>('POST', 'targets', input)
  }

  async function update(id: string, input: TargetInput): Promise<Target> {
    const t = await api<Target>('PUT', 'targets/' + id, input)
    paged.items.value = paged.items.value.map((x) => (x.id === id ? t : x))
    return t
  }

  async function remove(id: string): Promise<void> {
    await api('POST', 'targets/' + id + '/remove')
    paged.items.value = paged.items.value.filter((x) => x.id !== id)
  }

  async function attach(id: string, configurationIds: string[], overrides?: Record<string, Record<string, unknown>>): Promise<void> {
    await api('POST', 'targets/' + id + '/configurations', { configuration_ids: configurationIds, config_overrides: overrides })
  }

  async function detach(id: string, configurationIds: string[]): Promise<void> {
    await api('POST', 'targets/' + id + '/configurations/remove', { configuration_ids: configurationIds })
  }

  return { ...paged, get, create, update, remove, attach, detach }
})
