import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '@/api/client'

// Snapshot mirrors internal/stats.Snapshot (the /statistics/tenant response).
export interface Snapshot {
  jobs_by_status: Record<string, number>
  jobs_by_trigger: Record<string, number>
  jobs_total: number
  targets_total: number
  auto_deploy_targets: number
  configurations_total: number
  configurations_by_status: Record<string, number>
  configurations_by_provider: Record<string, number>
  success_rate_24h: number
  success_rate_7d: number
  recent_errors: { job_id: string; certificate_id: string; message: string; at: string }[]
}

export const useStats = defineStore('deployer-stats', () => {
  const snapshot = ref<Snapshot | null>(null)
  const loaded = ref(false)
  const error = ref('')

  // load fetches the tenant snapshot; a failure is non-fatal (the dashboard
  // falls back to figures derived from the loaded lists).
  async function load(): Promise<void> {
    try {
      snapshot.value = await api<Snapshot>('GET', 'statistics/tenant')
      loaded.value = true
    } catch (e) {
      error.value = (e as Error).message
    }
  }

  return { snapshot, loaded, error, load }
})
