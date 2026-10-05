import { defineStore } from 'pinia'
import type { ListParams, ListQueryOptions } from '@go-tangra/ui'
import { api } from '@/api/client'
import type { ActionResult, HistoryEntry, Job, JobResult, JobStatus } from '@/api/types'
import { pagedList, type Page } from '@/stores/paged'

/** Sortable fields of GET /jobs (server Spec store.JobList). */
export const JOB_SORTS = ['created_at', 'status', 'job_type', 'completed_at'] as const
export const JOB_LIST: ListQueryOptions = { sortable: [...JOB_SORTS], defaultSort: { key: 'created_at', dir: 'desc' }, defaultSize: 25 }
/** A job's children and history (server Specs ChildJobList / HistoryList). */
export const DETAIL_LIST: ListQueryOptions = { sortable: ['created_at'], defaultSort: { key: 'created_at', dir: 'desc' }, defaultSize: 10 }
const FIRST_PAGE: ListParams = { page: 1, page_size: 25, sort: 'created_at', order: 'desc' }

/** Debounce of the live reload when an event names a job not on the page. */
export const LIVE_RELOAD_MS = 300

export interface JobFilter extends Record<string, string | undefined> {
  status?: string | undefined
  triggered_by?: string | undefined
  certificate_id?: string | undefined
  job_type?: string | undefined
  target_id?: string | undefined
}

/** A live job event payload (deployment.started|completed|failed, job.updated). */
export interface JobEvent {
  job_id?: string
  id?: string
  parent_job_id?: string
  status?: JobStatus
  status_message?: string
  /** The cause of the last failure; "" once a deployment succeeds. */
  error?: string
  progress?: number
  retry_count?: number
  completed_at?: string | null
}

/** The job with the event's fields applied (fields the event omits are kept). */
export function patchJob(cur: Job, ev: JobEvent): Job {
  const next: Job = { ...cur }
  if (ev.status) next.status = ev.status
  if (typeof ev.progress === 'number') next.progress = ev.progress
  if (typeof ev.status_message === 'string') next.status_message = ev.status_message
  if (typeof ev.error === 'string') next.error = ev.error
  if (typeof ev.retry_count === 'number') next.retry_count = ev.retry_count
  if (typeof ev.completed_at === 'string') next.completed_at = ev.completed_at
  return next
}

export const useJobs = defineStore('deployer-jobs', () => {
  const paged = pagedList<Job, JobFilter>('jobs', FIRST_PAGE)

  async function result(id: string): Promise<JobResult> {
    return api<JobResult>('GET', 'jobs/' + id + '/result')
  }

  /** One page of a parent job's child jobs. */
  async function children(id: string, q: ListParams): Promise<Page<Job>> {
    return api<Page<Job>>('GET', 'jobs/' + id + '/children', undefined, { query: { ...q } })
  }

  /** One page of a job's deployment history. */
  async function history(id: string, q: ListParams): Promise<Page<HistoryEntry>> {
    return api<Page<HistoryEntry>>('GET', 'jobs/' + id + '/history', undefined, { query: { ...q } })
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

  // patch replaces one job on the current page (after cancel/retry).
  function patch(j: Job): void {
    const i = paged.items.value.findIndex((x) => x.id === j.id)
    if (i >= 0) paged.items.value[i] = j
  }

  let timer: ReturnType<typeof setTimeout> | null = null
  /** Reloads the current page (filter, page and order kept) after a quiet period. */
  function scheduleReload(): void {
    if (timer) clearTimeout(timer)
    timer = setTimeout(() => {
      timer = null
      void paged.reload()
    }, LIVE_RELOAD_MS)
  }
  function cancelReload(): void {
    if (timer) clearTimeout(timer)
    timer = null
  }

  /**
   * Applies a live event: a job on the current page has its status, progress,
   * message and error patched in place; any other job (new, or on another page) causes
   * a debounced reload of the current page so totals and order stay right.
   */
  function applyEvent(ev: JobEvent): void {
    const id = ev.job_id ?? ev.id
    if (!id) return
    const i = paged.items.value.findIndex((x) => x.id === id)
    if (i < 0) {
      scheduleReload()
      return
    }
    paged.items.value[i] = patchJob(paged.items.value[i]!, ev)
  }

  return { ...paged, result, children, history, cancel, retry, verify, rollback, patch, applyEvent, scheduleReload, cancelReload }
})
