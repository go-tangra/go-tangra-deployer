import { defineStore } from 'pinia'
import { ref } from 'vue'
import { useJobs, type JobEvent } from '@/stores/jobs'

// A single shared EventSource relays the module's live events through the
// gateway. deployment.completed / deployment.failed / job.updated patch a job
// on the current jobs page in place (status, progress); an event for a job not
// on the page reloads the page (debounced) so the server's order and total hold.
// The stream is reference-counted so several views share one connection.
export type Listener = (type: string, data: unknown) => void

const JOB_EVENTS = ['deployment.completed', 'deployment.failed', 'job.updated']

export const useLive = defineStore('deployer-live', () => {
  const connected = ref(false)
  let source: EventSource | null = null
  let refs = 0
  const listeners = new Set<Listener>()

  function handle(type: string, raw: string): void {
    let data: unknown = {}
    try {
      data = JSON.parse(raw)
    } catch {
      /* non-JSON payloads are ignored */
    }
    if (JOB_EVENTS.includes(type) && data && typeof data === 'object') useJobs().applyEvent(data as JobEvent)
    for (const l of listeners) l(type, data)
  }

  function open(): void {
    if (source) return
    source = new EventSource('/api/deployer/v1/stream', { withCredentials: true })
    source.onopen = () => (connected.value = true)
    source.onerror = () => (connected.value = false)
    for (const t of JOB_EVENTS) source.addEventListener(t, (e) => handle(t, (e as MessageEvent).data))
    source.addEventListener('message', (e) => handle((e as MessageEvent).type, (e as MessageEvent).data))
  }

  /** Opens the stream (first caller) and returns a release function. */
  function connect(): () => void {
    refs += 1
    open()
    return () => {
      refs -= 1
      if (refs <= 0) close()
    }
  }

  function close(): void {
    refs = 0
    useJobs().cancelReload()
    source?.close()
    source = null
    connected.value = false
  }

  function on(l: Listener): () => void {
    listeners.add(l)
    return () => listeners.delete(l)
  }

  // Exposed for tests: inject a fake event.
  function _emit(type: string, raw: string): void {
    handle(type, raw)
  }

  return { connected, connect, close, on, _emit }
})
