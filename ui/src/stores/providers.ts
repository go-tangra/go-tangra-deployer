import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '@/api/client'
import type { Provider } from '@/api/types'

export const useProviders = defineStore('deployer-providers', () => {
  const items = ref<Provider[]>([])
  const loaded = ref(false)
  const error = ref('')

  async function list(): Promise<void> {
    if (loaded.value) return
    try {
      const res = await api<{ items: Provider[] }>('GET', 'providers')
      items.value = res.items ?? []
      loaded.value = true
    } catch (e) {
      error.value = (e as Error).message
    }
  }

  function get(type: string): Provider | undefined {
    return items.value.find((p) => p.type === type)
  }

  return { items, loaded, error, list, get }
})
