import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { ListParams, ListQueryOptions } from '@go-tangra/ui'
import { api } from '@/api/client'
import type { Configuration, ConfigurationInput } from '@/api/types'
import { fetchAll, pagedList } from '@/stores/paged'

/** Sortable fields of GET /configurations (server Spec store.ConfigList). */
export const CONFIG_SORTS = ['name', 'provider_type', 'status', 'created_at'] as const
export const CONFIG_LIST: ListQueryOptions = { sortable: [...CONFIG_SORTS], defaultSort: { key: 'name', dir: 'asc' }, defaultSize: 25 }
const FIRST_PAGE: ListParams = { page: 1, page_size: 25, sort: 'name', order: 'asc' }

export interface ConfigFilter extends Record<string, string | undefined> {
  provider_type?: string | undefined
  status?: string | undefined
}

export const useConfigurations = defineStore('deployer-configurations', () => {
  const paged = pagedList<Configuration, ConfigFilter>('configurations', FIRST_PAGE)
  /** Every configuration (name order) for pickers such as target attachments. */
  const options = ref<Configuration[]>([])

  async function loadOptions(): Promise<void> {
    try {
      options.value = await fetchAll<Configuration>('configurations', 'name', 'asc')
    } catch (e) {
      paged.error.value = (e as Error).message
    }
  }

  async function create(input: ConfigurationInput): Promise<Configuration> {
    return api<Configuration>('POST', 'configurations', input)
  }

  async function update(id: string, input: ConfigurationInput): Promise<Configuration> {
    const c = await api<Configuration>('PUT', 'configurations/' + id, input)
    paged.items.value = paged.items.value.map((x) => (x.id === id ? c : x))
    return c
  }

  async function remove(id: string): Promise<void> {
    await api('POST', 'configurations/' + id + '/remove')
    paged.items.value = paged.items.value.filter((x) => x.id !== id)
  }

  async function validate(providerType: string, credentials: Record<string, unknown>, config?: Record<string, unknown>): Promise<void> {
    await api('POST', 'configurations/validate', { provider_type: providerType, credentials, config })
  }

  return { ...paged, options, loadOptions, create, update, remove, validate }
})
