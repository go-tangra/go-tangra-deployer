import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '@/api/client'
import type { Configuration, ConfigurationInput } from '@/api/types'

export const useConfigurations = defineStore('deployer-configurations', () => {
  const items = ref<Configuration[]>([])
  const loading = ref(false)
  const error = ref('')

  async function list(providerType?: string, status?: string): Promise<void> {
    loading.value = true
    error.value = ''
    try {
      const res = await api<{ items: Configuration[] }>('GET', 'configurations', undefined, {
        query: { provider_type: providerType, status },
      })
      items.value = res.items ?? []
    } catch (e) {
      error.value = (e as Error).message
    } finally {
      loading.value = false
    }
  }

  async function create(input: ConfigurationInput): Promise<Configuration> {
    const c = await api<Configuration>('POST', 'configurations', input)
    items.value = [c, ...items.value]
    return c
  }

  async function update(id: string, input: ConfigurationInput): Promise<Configuration> {
    const c = await api<Configuration>('PUT', 'configurations/' + id, input)
    items.value = items.value.map((x) => (x.id === id ? c : x))
    return c
  }

  async function remove(id: string): Promise<void> {
    await api('POST', 'configurations/' + id + '/remove')
    items.value = items.value.filter((x) => x.id !== id)
  }

  async function validate(providerType: string, credentials: Record<string, unknown>, config?: Record<string, unknown>): Promise<void> {
    await api('POST', 'configurations/validate', { provider_type: providerType, credentials, config })
  }

  return { items, loading, error, list, create, update, remove, validate }
})
