// Provider field descriptors → client-side validation, zod schemas, defaults
// and payloads (feature 033, contracts/deployer-config-ui.md §2-§6).
//
// The provider capability served by GET /providers is the single source of
// truth: no provider field list lives in the UI. validateInput/validateOverride
// mirror the Go validator (internal/provider/schema.go) rule by rule and code by
// code; both run the shared vectors api/testdata/provider-field-vectors.json so
// client and server cannot drift.
import { z } from 'zod'
import type { FieldType, Provider, ProviderField } from '@/api/types'

export type Mode = 'configuration' | 'effective'
/** Field path -> validation code ("required", "pattern", "too_long:32", …). Codes never carry values. */
export type FieldErrors = Record<string, string>

export const PATH_CONFIG = 'config.'
export const PATH_CREDENTIALS = 'credentials.'
export const PATH_OVERRIDES = 'config_overrides.'
export const DEFAULT_MAX_LENGTH = 1024

export const configFields = (c: Provider): ProviderField[] => c.config_fields ?? []
export const credentialFields = (c: Provider): ProviderField[] => c.credential_fields ?? []
export const oneOfRequired = (c: Provider): string[][] => c.one_of_required ?? []
export const fieldType = (f: ProviderField): FieldType => f.type ?? 'string'

// ---------------------------------------------------------------------------
// Value checks (Go: IsEmpty, checkValue, …)

const isList = (v: unknown): v is unknown[] => Array.isArray(v)
const isMap = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)

/** Missing for a required field: nil, blank string, empty list or empty map (v3 isEmptyValue). */
export function isEmpty(v: unknown): boolean {
  if (v === undefined || v === null) return true
  if (typeof v === 'string') return v.trim() === ''
  if (isList(v)) return v.length === 0
  if (isMap(v)) return Object.keys(v).length === 0
  return false
}

const encoder = new TextEncoder()
/** Byte length (Go len on a string). */
const byteLength = (s: string) => encoder.encode(s).length

const reCache = new Map<string, RegExp | null>()
/** The descriptor's RE2 pattern as a JS RegExp (anchored by the descriptor). */
function compiled(p: string): RegExp | null {
  if (reCache.has(p)) return reCache.get(p)!
  let re: RegExp | null = null
  try {
    re = new RegExp(p, 'u')
  } catch {
    try {
      re = new RegExp(p)
    } catch {
      re = null
    }
  }
  reCache.set(p, re)
  return re
}

// Header names refused in webhook custom headers (Go provider.IsAuthHeader).
const AUTH_HEADER_NAMES = new Set(['authorization', 'proxy-authorization', 'cookie', 'x-api-key'])
const AUTH_HEADER_PATTERN = /token|secret|key|auth|cookie|password|credential|jwt|session|bearer|signature/i
export function isAuthHeader(name: string): boolean {
  const n = name.trim().toLowerCase()
  return AUTH_HEADER_NAMES.has(n) || AUTH_HEADER_PATTERN.test(n)
}

/** A submitted key made safe for an error path: ≤ 64 bytes, [A-Za-z0-9_-] only. */
export function safeKey(k: string): string {
  const b = encoder.encode(k).slice(0, 64)
  let out = ''
  for (const c of b) out += (c >= 97 && c <= 122) || (c >= 65 && c <= 90) || (c >= 48 && c <= 57) || c === 95 || c === 45 ? String.fromCharCode(c) : '_'
  return out
}

function checkString(f: { max_length?: number | undefined; pattern?: string | undefined }, s: string, withPattern: boolean): string {
  const limit = f.max_length && f.max_length > 0 ? f.max_length : DEFAULT_MAX_LENGTH
  if (byteLength(s) > limit) return 'too_long:' + limit
  if (withPattern && f.pattern) {
    const re = compiled(f.pattern)
    if (!re || !re.test(s)) return 'pattern'
  }
  return ''
}

function rangeCode(f: ProviderField): string {
  return 'out_of_range:' + (f.min ?? '') + '..' + (f.max ?? '')
}

function validURL(s: string): boolean {
  let u: URL
  try {
    u = new URL(s)
  } catch {
    return false
  }
  if (u.protocol !== 'http:' && u.protocol !== 'https:') return false
  if (!u.hostname) return false
  // Go url.URL.User is set by any "@" in the authority.
  return !/^[a-z][a-z0-9+.-]*:\/\/[^/?#]*@/i.test(s)
}

/** The code of the first rule v violates, or "". Empty values pass (required-ness is separate). */
export function checkValue(f: ProviderField, v: unknown): string {
  const t = fieldType(f)
  if (isEmpty(v)) {
    if (v === undefined || v === null || typeof v === 'string') return ''
    if ((t === 'string_list' || t === 'host_selector') && isList(v)) return ''
    if (t === 'key_value' && isMap(v)) return ''
    return 'wrong_type'
  }
  switch (t) {
    case 'string':
    case 'text':
      return typeof v === 'string' ? checkString(f, v, t === 'string') : 'wrong_type'
    case 'url': {
      if (typeof v !== 'string') return 'wrong_type'
      const code = checkString(f, v, false)
      if (code) return code
      return validURL(v) ? '' : 'invalid_url'
    }
    case 'int': {
      if (typeof v !== 'number' || !Number.isInteger(v) || Math.abs(v) > 2 ** 53) return 'wrong_type'
      if ((f.min !== undefined && v < f.min) || (f.max !== undefined && v > f.max)) return rangeCode(f)
      return ''
    }
    case 'bool':
      return typeof v === 'boolean' ? '' : 'wrong_type'
    case 'enum':
      if (typeof v !== 'string') return 'wrong_type'
      return (f.options ?? []).some((o) => o.value === v) ? '' : 'not_in_options'
    case 'string_list':
    case 'host_selector': {
      if (!isList(v) || v.some((it) => typeof it !== 'string')) return 'wrong_type'
      if (f.max_items && f.max_items > 0 && v.length > f.max_items) return 'too_many_items:' + f.max_items
      for (const it of v as string[]) {
        const code = checkString(f, it, true)
        if (code) return code
      }
      return ''
    }
    case 'key_value': {
      if (!isMap(v) || Object.values(v).some((it) => typeof it !== 'string')) return 'wrong_type'
      const names = Object.keys(v).sort()
      if (f.max_items && f.max_items > 0 && names.length > f.max_items) return 'too_many_items:' + f.max_items
      for (const k of names) {
        if (f.key === 'headers' && isAuthHeader(k)) return 'forbidden_header:' + safeKey(k)
        const code = checkString({ max_length: f.max_length }, v[k] as string, false)
        if (code) return code
      }
      return ''
    }
  }
  return 'wrong_type'
}

// ---------------------------------------------------------------------------
// Input and override validation (Go: ValidateInput, ValidateOverride)

export interface ValidateOptions {
  /** Credential keys stored server-side (edit: blank secrets keep them), counted as present. */
  storedCredentials?: readonly string[]
}

function checkValues(fields: ProviderField[], m: Record<string, unknown>, prefix: string, errs: FieldErrors): void {
  const byKey = new Map(fields.map((f) => [f.key, f]))
  for (const [k, v] of Object.entries(m)) {
    const f = byKey.get(k)
    if (!f) {
      errs[prefix + safeKey(k)] = 'unknown_field'
      continue
    }
    const code = checkValue(f, v)
    if (code) errs[prefix + k] = code
  }
}

const groupEmpty = (g: string[], m: Record<string, unknown>) => g.every((k) => isEmpty(m[k]))
const groupOverridable = (c: Provider, g: string[]) => g.every((k) => configFields(c).find((f) => f.key === k)?.overridable)
const groupCode = (g: string[]) => 'one_of_required:' + g.join(',')

/**
 * Checks a configuration and its credentials against the provider's
 * descriptors. In configuration mode a missing overridable required field (or
 * a fully overridable empty one_of_required group) is not an error: those keys
 * are returned as targetSupplied. A null creds skips the credential checks.
 */
export function validateInput(c: Provider, config: Record<string, unknown>, creds: Record<string, unknown> | null, mode: Mode, opts: ValidateOptions = {}): { errors: FieldErrors; targetSupplied: string[] } {
  const errors: FieldErrors = {}
  const targetSupplied: string[] = []
  checkValues(configFields(c), config, PATH_CONFIG, errors)
  for (const f of configFields(c)) {
    if (!f.required || !isEmpty(config[f.key])) continue
    if (mode === 'configuration' && f.overridable) {
      targetSupplied.push(f.key)
      continue
    }
    errors[PATH_CONFIG + f.key] ??= 'required'
  }
  for (const g of oneOfRequired(c)) {
    if (!groupEmpty(g, config)) continue
    if (mode === 'configuration' && groupOverridable(c, g)) {
      targetSupplied.push(...g)
      continue
    }
    for (const k of g) errors[PATH_CONFIG + k] ??= groupCode(g)
  }
  if (creds) {
    checkValues(credentialFields(c), creds, PATH_CREDENTIALS, errors)
    const stored = new Set(opts.storedCredentials ?? [])
    for (const f of credentialFields(c)) {
      if (f.required && isEmpty(creds[f.key]) && !stored.has(f.key)) errors[PATH_CREDENTIALS + f.key] = 'required'
    }
  }
  return { errors, targetSupplied }
}

/** Keys every target attaching this configuration must supply (declaration order). */
export function targetSupplied(c: Provider, config: Record<string, unknown>): string[] {
  return validateInput(c, config, null, 'configuration').targetSupplied
}

/** Overlays a target override on a configuration; empty override values mean "inherit". */
export function mergeOverride(config: Record<string, unknown>, override: Record<string, unknown>): Record<string, unknown> {
  const out = { ...config }
  for (const [k, v] of Object.entries(override)) if (!isEmpty(v)) out[k] = v
  return out
}

/** Checks a target override for one attached configuration (contracts §4a). */
export function validateOverride(c: Provider, config: Record<string, unknown>, override: Record<string, unknown>): FieldErrors {
  const errors: FieldErrors = {}
  const fields = configFields(c)
  for (const [k, v] of Object.entries(override)) {
    const f = fields.find((x) => x.key === k)
    if (!f) {
      if (credentialFields(c).some((x) => x.key === k)) errors[PATH_OVERRIDES + k] = 'not_overridable'
      else errors[PATH_OVERRIDES + safeKey(k)] = 'unknown_field'
    } else if (!f.overridable) {
      errors[PATH_OVERRIDES + k] = 'not_overridable'
    } else if (!isEmpty(v)) {
      const code = checkValue(f, v)
      if (code) errors[PATH_OVERRIDES + k] = code
    }
  }
  const merged = mergeOverride(config, override)
  const requiredPath = (f: ProviderField) => (f.overridable ? PATH_OVERRIDES : PATH_CONFIG) + f.key
  for (const f of fields) if (f.required && isEmpty(merged[f.key])) errors[requiredPath(f)] = 'required'
  for (const g of oneOfRequired(c)) {
    if (!groupEmpty(g, merged)) continue
    for (const k of g) {
      const f = fields.find((x) => x.key === k)!
      errors[requiredPath(f)] ??= groupCode(g)
    }
  }
  return errors
}

// ---------------------------------------------------------------------------
// Wording

const fieldByKey = (c: Provider, key: string) => [...configFields(c), ...credentialFields(c)].find((f) => f.key === key)

/** Labels of the given config keys, in declaration order. */
export function labels(c: Provider | undefined, keys: readonly string[]): string[] {
  if (!c) return [...keys]
  const want = new Set(keys)
  const out = configFields(c).filter((f) => want.has(f.key)).map((f) => f.label)
  return out.length ? out : [...keys]
}

function oneOfText(c: Provider | undefined, keys: string[]): string {
  const parts = keys.map((k) => {
    const f = c ? fieldByKey(c, k) : undefined
    if (!f) return 'enter ' + k
    if (fieldType(f) === 'host_selector') return 'select ' + f.label.toLowerCase()
    return 'enter ' + f.label.toLowerCase()
  })
  const s = parts.join(' or ')
  return s.charAt(0).toUpperCase() + s.slice(1) + '.'
}

/** Human text for a validation code (built from the descriptor only — never a submitted value). */
export function describeCode(code: string, c?: Provider, key?: string): string {
  const [name, arg = ''] = code.split(/:(.*)/s)
  const f = c && key ? fieldByKey(c, key) : undefined
  switch (name) {
    case 'required':
      return 'This field is required.'
    case 'one_of_required':
      return oneOfText(c, arg.split(','))
    case 'wrong_type':
      return 'This value has the wrong type.'
    case 'pattern':
      return f?.placeholder ? `This value has the wrong format (example: ${f.placeholder}).` : 'This value has the wrong format.'
    case 'too_long':
      return `Must be at most ${arg} characters.`
    case 'out_of_range': {
      const [lo, hi] = arg.split('..')
      if (lo && hi) return `Must be between ${lo} and ${hi}.`
      return lo ? `Must be at least ${lo}.` : `Must be at most ${hi}.`
    }
    case 'too_many_items':
      return `At most ${arg} entries.`
    case 'not_in_options':
      return 'Choose one of the listed values.'
    case 'invalid_url':
      return 'Enter an http(s) URL without a user name or password.'
    case 'unknown_field':
      return 'Not a setting of this provider.'
    case 'forbidden_header':
      return `The header ${arg} carries credentials; use the credential fields instead.`
    case 'not_overridable':
      return 'A deployment target cannot override this setting.'
    case 'required_by_targets':
      return 'Deployment targets rely on this value.'
    case 'not_found_on_endpoint':
      return 'Not found on the endpoint.'
  }
  return 'This value is not valid.'
}

/** The key of a field path ("config.zone_id" → "zone_id"). */
export const keyOf = (path: string) => path.slice(path.indexOf('.') + 1)

// ---------------------------------------------------------------------------
// Zod schemas over the descriptors (the form's single validation mechanism)

export interface ProviderPayload {
  config: Record<string, unknown>
  credentials?: Record<string, unknown>
}

function issuesFrom(ctx: z.RefinementCtx, errors: FieldErrors, c: Provider, prefix: string): void {
  for (const [path, code] of Object.entries(errors)) {
    ctx.addIssue({ code: 'custom', path: [prefix + path], message: describeCode(code, c, keyOf(path)), params: { code } })
  }
}

export interface FieldsToZodOptions extends ValidateOptions {
  mode?: Mode
  /** Prepended to every issue path (several forms on one page). */
  idPrefix?: string
}

/**
 * The zod schema of a configuration payload ({config, credentials}) generated
 * from the descriptors. Issues carry the server's field paths
 * ("config.<key>", "credentials.<key>") and the code in `params.code`; the
 * output adds the target-supplied keys.
 */
export function fieldsToZod(c: Provider, opts: FieldsToZodOptions = {}) {
  const prefix = opts.idPrefix ?? ''
  return z
    .object({ config: z.record(z.string(), z.unknown()).default({}), credentials: z.record(z.string(), z.unknown()).optional() })
    .transform((v, ctx) => {
      const r = validateInput(c, v.config, v.credentials ?? null, opts.mode ?? 'configuration', opts)
      if (Object.keys(r.errors).length) {
        issuesFrom(ctx, r.errors, c, prefix)
        return z.NEVER
      }
      return { config: v.config, ...(v.credentials ? { credentials: v.credentials } : {}), target_supplied: r.targetSupplied }
    })
}

/**
 * The zod schema of one target override against a configuration: issues at
 * "config_overrides.<key>" (or "config.<key>" for a required field a target
 * cannot supply); the output drops empty values (inherit).
 */
export function overrideToZod(c: Provider, config: Record<string, unknown>, opts: { idPrefix?: string } = {}) {
  const prefix = opts.idPrefix ?? ''
  return z.record(z.string(), z.unknown()).transform((override, ctx) => {
    const errors = validateOverride(c, config, override)
    if (Object.keys(errors).length) {
      issuesFrom(ctx, errors, c, prefix)
      return z.NEVER
    }
    return Object.fromEntries(Object.entries(override).filter(([, v]) => !isEmpty(v)))
  })
}

// ---------------------------------------------------------------------------
// Form values ↔ payloads. Form values are flat: "config.<key>" and
// "credentials.<key>" (or "config_overrides.<key>"), optionally prefixed.

/** The empty form value of a field type. */
export function emptyValue(f: ProviderField): unknown {
  switch (fieldType(f)) {
    case 'bool':
      return false
    case 'string_list':
    case 'host_selector':
      return []
    case 'key_value':
      return {}
    default:
      return ''
  }
}

/** A bool's value when nothing usable is stored: its default (as the server reads it). */
function boolDefault(f: ProviderField): boolean {
  return typeof f.default === 'boolean' ? f.default : false
}

/** A legacy string bool, read as the providers do (v3 cfgBool); anything else is the default. */
function legacyBool(f: ProviderField, v: string): boolean {
  const s = v.trim().toLowerCase()
  if (['true', '1', 'yes', 'on'].includes(s)) return true
  if (['false', '0', 'no', 'off'].includes(s)) return false
  return boolDefault(f)
}

/**
 * Form value from a stored value (wrong-typed legacy values become empty). A
 * bool that is not stored keeps its default, so saving an edit never flips a
 * default-on option off.
 */
function formValue(f: ProviderField, v: unknown): unknown {
  if (v === undefined || v === null) return fieldType(f) === 'bool' ? boolDefault(f) : emptyValue(f)
  switch (fieldType(f)) {
    case 'bool':
      return typeof v === 'boolean' ? v : typeof v === 'string' ? legacyBool(f, v) : boolDefault(f)
    case 'int':
      return typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' && !Number.isNaN(Number(v)) ? Number(v) : ''
    case 'string_list':
    case 'host_selector':
      return Array.isArray(v) ? v.map(String) : emptyValue(f)
    case 'key_value':
      return isMap(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, String(x)])) : {}
    default:
      return typeof v === 'string' ? v : String(v)
  }
}

/**
 * Initial form values for a provider: on create the descriptor defaults, on
 * edit the stored config and the non-secret credential values. Secrets always
 * start empty.
 */
export function initialValues(c: Provider, stored?: { config?: Record<string, unknown> | undefined; credentials_public?: Record<string, unknown> | undefined }, prefix = ''): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const f of configFields(c)) out[prefix + PATH_CONFIG + f.key] = stored ? formValue(f, stored.config?.[f.key]) : f.default !== undefined ? formValue(f, f.default) : emptyValue(f)
  for (const f of credentialFields(c)) {
    let v = emptyValue(f)
    if (!f.secret) v = stored ? formValue(f, stored.credentials_public?.[f.key]) : f.default !== undefined ? formValue(f, f.default) : v
    out[prefix + PATH_CREDENTIALS + f.key] = v
  }
  return out
}

/** A form value → its JSON value, or undefined when it is empty (not sent). */
function payloadValue(f: ProviderField, v: unknown): unknown {
  if (fieldType(f) === 'bool') return typeof v === 'boolean' ? v : undefined
  if (fieldType(f) === 'int' && (v === '' || v === undefined || v === null)) return undefined
  if (fieldType(f) === 'string_list' || fieldType(f) === 'host_selector') {
    const items = Array.isArray(v) ? v.map((x) => String(x).trim()).filter(Boolean) : []
    return items.length ? items : undefined
  }
  return isEmpty(v) ? undefined : v
}

/**
 * The configuration payload of a form: declared keys only (legacy undeclared
 * keys are dropped), empty values left out (blank secrets keep the stored
 * value on edit).
 */
export function buildPayload(c: Provider, values: Record<string, unknown>, prefix = ''): Required<ProviderPayload> {
  const config: Record<string, unknown> = {}
  for (const f of configFields(c)) {
    const v = payloadValue(f, values[prefix + PATH_CONFIG + f.key])
    if (v !== undefined) config[f.key] = v
  }
  const credentials: Record<string, unknown> = {}
  for (const f of credentialFields(c)) {
    const v = payloadValue(f, values[prefix + PATH_CREDENTIALS + f.key])
    if (v !== undefined) credentials[f.key] = v
  }
  return { config, credentials }
}

/** Override form values of the overridable fields, from a stored override. */
export function overrideValues(c: Provider, override: Record<string, unknown> | undefined, prefix = ''): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const f of overridableFields(c)) {
    const v = override?.[f.key]
    // A boolean override is a three-state choice: "" (inherit), "true", "false".
    out[prefix + PATH_OVERRIDES + f.key] = fieldType(f) === 'bool' ? (typeof v === 'boolean' ? String(v) : '') : v === undefined ? emptyValue(f) : formValue(f, v)
  }
  return out
}

/** The override payload of a form: only non-empty values of overridable fields. */
export function buildOverride(c: Provider, values: Record<string, unknown>, prefix = ''): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const f of overridableFields(c)) {
    const raw = values[prefix + PATH_OVERRIDES + f.key]
    const v = fieldType(f) === 'bool' ? (raw === 'true' ? true : raw === 'false' ? false : undefined) : payloadValue(f, raw)
    if (v !== undefined) out[f.key] = v
  }
  return out
}

export const overridableFields = (c: Provider) => configFields(c).filter((f) => f.overridable)

/** Keys of a stored config that the provider does not declare (legacy rows). */
export function unknownKeys(c: Provider, config: Record<string, unknown> | undefined): string[] {
  const declared = new Set(configFields(c).map((f) => f.key))
  return Object.keys(config ?? {}).filter((k) => !declared.has(k)).sort()
}

/** Whether every Options value equals its default (the section starts collapsed). */
export function optionsAtDefaults(c: Provider, values: Record<string, unknown>, prefix = ''): boolean {
  return configFields(c)
    .filter((f) => f.group === 'options')
    .every((f) => {
      const v = payloadValue(f, values[prefix + PATH_CONFIG + f.key])
      return v === undefined ? f.default === undefined || f.default === false : JSON.stringify(v) === JSON.stringify(f.default)
    })
}

/** The label of the validate action (contracts §1). */
export function validateLabel(c: Provider): string {
  if (configFields(c).some((f) => fieldType(f) === 'host_selector')) return 'Preview hosts'
  return c.test_connection ? 'Test connection' : 'Check settings'
}
