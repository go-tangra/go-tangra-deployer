import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '@/api/client'
import type { ActionResult, Job, JobResult } from '@/api/types'

export interface JobFilter {
  status?: string | undefined
  triggered_by?: string | undefined
  certificate_id?: string | undefined
  job_type?: string | undefined
}

export const useJobs = defineStore('deployer-jobs', () => {
  const items = ref<Job[]>([])
  const loading = ref(false)
  const error = ref('')

  async function list(filter: JobFilter = {}): Promise<void> {
    loading.value = true
    error.value = ''
    try {
      const res = await api<{ items: Job[] }>('GET', 'jobs', undefined, { query: { ...filter } })
      items.value = res.items ?? []
    } catch (e) {
      error.value = (e as Error).message
    } finally {
      loading.value = false
    }
  }

  async function result(id: string): Promise<JobResult> {
    return api<JobResult>('GET', 'jobs/' + id + '/result')
  }

  async function cancel(id: string): Promise<Job> {
    const j = await api<Job>('POST', 'jobs/' + id + '/cancel')
    patch(j)
    return j
  }

  async function retry(id: string, force = false): Promise<Job> {
    const j = await api<Job>('POST', 'jobs/' + id + '/retry', undefined, { query: { force } })
    patch(j)
    return j
  }

  async function verify(jobId: string): Promise<ActionResult> {
    return api<ActionResult>('POST', 'deploy/' + jobId + '/verify')
  }

  async function rollback(jobId: string): Promise<ActionResult> {
    return api<ActionResult>('POST', 'deploy/' + jobId + '/rollback')
  }

  // patch replaces one job in the list (used after cancel/retry/live updates).
  function patch(j: Job): void {
    const i = items.value.findIndex((x) => x.id === j.id)
    if (i >= 0) items.value[i] = j
  }

  return { items, loading, error, list, result, cancel, retry, verify, rollback, patch }
})
