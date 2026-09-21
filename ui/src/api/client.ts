import type { paths } from './schema.d'

// Path names are checked against the OpenAPI contract at compile time.
export type ApiPath = keyof paths
export type Method = 'GET' | 'POST' | 'PUT' | 'DELETE'

export const CSRF_COOKIE = '__Host-csrf'
export const CSRF_HEADER = 'X-CSRF-Token'
export const BASE = '/api/deployer/v1'

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly reason: string,
  ) {
    super(reason)
    this.name = 'ApiError'
  }
}

/** Reads the double-submit CSRF cookie issued by the gateway edge. */
export function csrfToken(): string {
  const prefix = CSRF_COOKIE + '='
  const hit = document.cookie.split('; ').find((c) => c.startsWith(prefix))
  return hit ? decodeURIComponent(hit.slice(prefix.length)) : ''
}

export interface RequestOptions {
  signal?: AbortSignal
  query?: Record<string, string | number | boolean | undefined>
}

/**
 * Calls the notification API through the gateway. Non-2xx responses reject with
 * ApiError carrying the server's closed-vocabulary `reason`.
 */
export async function api<T = unknown>(method: Method, path: string, body?: unknown, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' }
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  if (method !== 'GET') headers[CSRF_HEADER] = csrfToken()
  let url = path.startsWith('/') ? path : BASE + '/' + path
  if (opts.query) {
    const q = new URLSearchParams()
    for (const [k, v] of Object.entries(opts.query)) if (v !== undefined && v !== '') q.set(k, String(v))
    const s = q.toString()
    if (s) url += (url.includes('?') ? '&' : '?') + s
  }
  let res: Response
  try {
    res = await fetch(url, {
      method,
      headers,
      credentials: 'same-origin',
      body: body === undefined ? null : JSON.stringify(body),
      ...(opts.signal ? { signal: opts.signal } : {}),
    })
  } catch (err) {
    if (err instanceof DOMException && err.name === 'AbortError') throw err
    throw new ApiError(0, 'network')
  }
  if (res.status === 204) return undefined as T
  const data: unknown = await res.json().catch(() => ({}))
  if (!res.ok) {
    const reason = typeof data === 'object' && data !== null && 'reason' in data ? String((data as { reason: unknown }).reason) : 'error'
    throw new ApiError(res.status, reason)
  }
  return data as T
}

/** Human wording for the closed reason vocabulary. */
export function describe(err: unknown): string {
  if (!(err instanceof ApiError)) return 'Something went wrong.'
  switch (err.reason) {
    case 'forbidden':
      return 'You are not allowed to do that.'
    case 'not_found':
      return 'Not found.'
    case 'conflict':
      return 'A sibling with that name already exists, or the move is not possible.'
    case 'validation_failed':
      return 'Please check the highlighted fields.'
    case 'vault_unavailable':
      return 'The vault is unavailable; metadata stays readable, material does not.'
    case 'rate_limited':
      return 'Too many requests; try again shortly.'
    case 'network':
    case 'temporarily_unavailable':
      return 'The service is temporarily unavailable.'
    default:
      return 'Request refused (' + err.reason + ').'
  }
}
