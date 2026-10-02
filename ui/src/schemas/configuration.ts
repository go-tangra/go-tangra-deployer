import { z } from 'zod'
import { nonEmpty, optionalString } from '@go-tangra/ui/forms'
import type { ConfigurationInput, Provider } from '@/api/types'
import { buildPayload, describeCode, fieldsToZod, keyOf } from '@/schemas/providerFields'

/** The provider-independent part of POST/PUT /configurations. */
export const configurationBaseSchema = z.object({
  name: nonEmpty(200),
  description: optionalString(2000),
  provider_type: nonEmpty(64),
})

export interface ConfigurationFormContext {
  /** The provider's capabilities (undefined until a provider is chosen). */
  provider: () => Provider | undefined
  /** Stored credential names (edit). */
  credentialsSet: () => readonly string[]
  /** Optional credentials to remove (edit). */
  cleared: () => readonly string[]
}

export type ConfigurationFormOutput = ConfigurationInput & { target_supplied: string[] }

/**
 * The configuration drawer's schema: the base fields plus the provider's
 * descriptor schema (fieldsToZod) over the flat form values ("config.<key>",
 * "credentials.<key>"). The output is the request payload: declared non-empty
 * keys only, blank secrets left out (kept on edit), clear_credentials on edit.
 * Credentials are write-only: sent when given, never echoed.
 */
export function configurationSchema(ctx: ConfigurationFormContext) {
  return z.record(z.string(), z.unknown()).transform((values, issues): ConfigurationFormOutput => {
    const base = configurationBaseSchema.safeParse(values)
    if (!base.success) for (const i of base.error.issues) issues.addIssue({ ...i } as never)
    const p = ctx.provider()
    if (!p) return z.NEVER
    const cleared = ctx.cleared()
    const payload = buildPayload(p, values)
    const stored = ctx.credentialsSet().filter((k) => !cleared.includes(k))
    const r = fieldsToZod(p, { mode: 'configuration', storedCredentials: stored }).safeParse(payload)
    if (!r.success) for (const i of r.error.issues) issues.addIssue({ code: 'custom', path: i.path, message: i.message, params: (i as { params?: Record<string, unknown> }).params ?? {} })
    if (!base.success || !r.success) return z.NEVER
    return {
      name: base.data.name,
      description: base.data.description,
      provider_type: base.data.provider_type,
      config: payload.config,
      ...(Object.keys(payload.credentials).length ? { credentials: payload.credentials } : {}),
      ...(cleared.length ? { clear_credentials: [...cleared] } : {}),
      target_supplied: r.data.target_supplied,
    }
  })
}

const KNOWN = /^(required|one_of_required|wrong_type|pattern|too_long|out_of_range|too_many_items|not_in_options|invalid_url|unknown_field|forbidden_header|not_overridable|required_by_targets|not_found_on_endpoint)(:|$)/

/**
 * Server field refusals as inline texts: codes are worded from the
 * descriptor; required_by_targets names the targets that rely on the value;
 * anything else is the server's fixed client-safe text.
 */
export function describeFieldErrors(fields: Record<string, unknown>, p: Provider | undefined, targets?: { name?: string }[], prefix = ''): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [path, raw] of Object.entries(fields)) {
    const code = typeof raw === 'string' ? raw : ''
    let text: string
    if (code === 'required_by_targets') {
      const names = (targets ?? []).map((t) => t.name).filter(Boolean)
      text = names.length ? `Deployment targets rely on this value: ${names.join(', ')}.` : describeCode(code)
    } else if (KNOWN.test(code)) {
      text = describeCode(code, p, keyOf(path))
    } else {
      text = code || 'This value is not valid.'
    }
    out[prefix + path] = text
  }
  return out
}
