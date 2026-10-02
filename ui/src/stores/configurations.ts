import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { ListParams, ListQueryOptions } from '@go-tangra/ui'
import { api } from '@/api/client'
import { registerReasons } from '@go-tangra/ui/forms'
import type { Configuration, ConfigurationInput, ValidateResult } from '@/api/types'
import { fetchAll, pagedList } from '@/stores/paged'

// The deployer's own refusal reasons (closed vocabulary; server detail never shown).
registerReasons({
  credentials_rejected: 'The endpoint refused these settings or credentials.',
  provider_type: 'The provider of a configuration cannot be changed.',
})

/** POST /configurations/validate body: on edit configuration_id merges the stored credentials. */
export interface ValidateInput {
  provider_type: string
  configuration_id?: string
  config: Record<string, unknown>
  credentials: Record<string, unknown>
}

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

  /** A single configuration; managers also get credentials_set / credentials_public. */
  async function get(id: string): Promise<Configuration> {
    return api<Configuration>('GET', 'configurations/' + id)
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

  /** Test connection / Check settings / Preview hosts: nothing is saved. */
  async function validate(input: ValidateInput): Promise<ValidateResult> {
    return api<ValidateResult>('POST', 'configurations/validate', input)
  }

  /** Deploys a certificate to one configuration directly (a "direct" job). */
  async function deploy(certificateId: string, configurationId: string): Promise<string> {
    const res = await api<{ job_id: string }>('POST', 'deploy', { certificate_id: certificateId, configuration_id: configurationId })
    return res.job_id
  }

  return { ...paged, options, loadOptions, get, create, update, remove, validate, deploy }
})
