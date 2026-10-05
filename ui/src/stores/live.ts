import { defineStore } from 'pinia'
import { ref } from 'vue'
import { useJobs, type JobEvent } from '@/stores/jobs'

// Job events reach the UI over the platform's realtime bus: the deployer
// publishes them to the tenant's shared Valkey stream and the gateway relays
// that stream to the browser (GET /gateway/v1/stream). Inside the shell the
// module subscribes through the shell's shared connection (the BootContext
// `live` bus handed to ./boot); standalone (vite dev, an older shell) it opens
// its own EventSource on the same endpoint.
//
// Every job event patches the job on the current jobs page in place (status,
// progress, message, error); an event for a job not on the page reloads the
// page (debounced) so the server's order and total hold. Views add their own
// listeners with on(). The subscription is reference-counted.
export type Listener = (type: string, data: unknown) => void

/** The shell's shared realtime bus (BootContext.live). */
export interface PlatformBus {
  on(type: string, fn: (data: unknown) => void): () => void
}

/** The gateway's platform event stream. */
export const PLATFORM_STREAM = '/gateway/v1/stream'

export const JOB_EVENTS = ['deployment.started', 'deployment.completed', 'deployment.failed', 'job.updated']

let platformBus: PlatformBus | null = null

/** Hands the shell's bus to the module (called by ./boot). */
export function setPlatformBus(bus: PlatformBus | null | undefined): void {
  platformBus = bus ?? null
}

export const useLive = defineStore('deployer-live', () => {
  const connected = ref(false)
  let source: EventSource | null = null
  let unsubs: (() => void)[] = []
  let open = false
  let refs = 0
  const listeners = new Set<Listener>()

  function deliver(type: string, data: unknown): void {
    if (JOB_EVENTS.includes(type) && data && typeof data === 'object') useJobs().applyEvent(data as JobEvent)
    for (const l of listeners) {
      try {
        l(type, data)
      } catch {
        /* a listener throwing must not break the others */
      }
    }
  }

  function handle(type: string, raw: string): void {
    let data: unknown = {}
    try {
      data = JSON.parse(raw)
    } catch {
      /* non-JSON payloads are ignored */
    }
    deliver(type, data)
  }

  function start(): void {
    if (open) return
    open = true
    if (platformBus) {
      // The shell owns the connection (and its reconnects).
      const bus = platformBus
      unsubs = JOB_EVENTS.map((t) => bus.on(t, (data) => deliver(t, data)))
      connected.value = true
      return
    }
    source = new EventSource(PLATFORM_STREAM, { withCredentials: true })
    source.onopen = () => (connected.value = true)
    source.onerror = () => (connected.value = false)
    for (const t of JOB_EVENTS) source.addEventListener(t, (e) => handle(t, (e as MessageEvent).data))
  }

  /** Subscribes (first caller) and returns a release function. */
  function connect(): () => void {
    refs += 1
    start()
    let released = false
    return () => {
      if (released) return
      released = true
      refs -= 1
      if (refs <= 0) close()
    }
  }

  function close(): void {
    refs = 0
    open = false
    useJobs().cancelReload()
    for (const u of unsubs) u()
    unsubs = []
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
