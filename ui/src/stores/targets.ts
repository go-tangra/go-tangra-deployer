import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '@/api/client'
import type { Target, TargetInput } from '@/api/types'

export const useTargets = defineStore('deployer-targets', () => {
  const items = ref<Target[]>([])
  const loading = ref(false)
  const error = ref('')

  async function list(): Promise<void> {
    loading.value = true
    error.value = ''
    try {
      const res = await api<{ items: Target[] }>('GET', 'targets')
      items.value = res.items ?? []
    } catch (e) {
      error.value = (e as Error).message
    } finally {
      loading.value = false
    }
  }

  async function create(input: TargetInput): Promise<Target> {
    const t = await api<Target>('POST', 'targets', input)
    items.value = [t, ...items.value]
    return t
  }

  async function update(id: string, input: TargetInput): Promise<Target> {
    const t = await api<Target>('PUT', 'targets/' + id, input)
    items.value = items.value.map((x) => (x.id === id ? t : x))
    return t
  }

  async function remove(id: string): Promise<void> {
    await api('POST', 'targets/' + id + '/remove')
    items.value = items.value.filter((x) => x.id !== id)
  }

  async function attach(id: string, configurationIds: string[], overrides?: Record<string, Record<string, unknown>>): Promise<void> {
    await api('POST', 'targets/' + id + '/configurations', { configuration_ids: configurationIds, config_overrides: overrides })
  }

  async function detach(id: string, configurationIds: string[]): Promise<void> {
    await api('POST', 'targets/' + id + '/configurations/remove', { configuration_ids: configurationIds })
  }

  return { items, loading, error, list, create, update, remove, attach, detach }
})
