import { z } from 'zod'
import type { CertificateFilter } from '@/api/types'
import { nonEmpty, optionalString } from '@go-tangra/ui/forms'
import type { Configuration, Provider } from '@/api/types'
import { buildOverride, overrideToZod } from '@/schemas/providerFields'

/** One AND-matched certificate filter; blank rules are dropped. */
const regexRule = optionalString(500).pipe(
  z.string().optional().refine((s) => {
    if (!s) return true
    try {
      new RegExp(s)
      return true
    } catch {
      return false
    }
  }, 'Enter a valid regular expression.'),
)
export const certificateFilterSchema = z.object({
  issuer: optionalString(500),
  common_name: regexRule,
  san: regexRule,
  organization: optionalString(500),
  // No inputs: carried through so a save never drops them from a stored filter.
  org_unit: optionalString(500),
  country: optionalString(500),
})

/** A stored filter as form values (the form uses short field names). */
export function filterToForm(f: CertificateFilter): Record<string, string> {
  return { issuer: f.issuer_name ?? '', common_name: f.common_name_pattern ?? '', san: f.san_pattern ?? '', organization: f.subject_organization ?? '', org_unit: f.subject_org_unit ?? '', country: f.subject_country ?? '' }
}

/** Form filter values as the API stores them; empty fields are left out. */
export function filterToApi(f: { issuer?: string | undefined; common_name?: string | undefined; san?: string | undefined; organization?: string | undefined; org_unit?: string | undefined; country?: string | undefined }): CertificateFilter {
  const out: CertificateFilter = {}
  if (f.issuer) out.issuer_name = f.issuer
  if (f.common_name) out.common_name_pattern = f.common_name
  if (f.san) out.san_pattern = f.san
  if (f.organization) out.subject_organization = f.organization
  if (f.org_unit) out.subject_org_unit = f.org_unit
  if (f.country) out.subject_country = f.country
  return out
}

/** POST/PUT /targets payload plus the attached configuration ids. */
export const targetSchema = z.object({
  name: nonEmpty(200),
  description: optionalString(2000),
  auto_deploy: z.boolean().optional().transform((v) => v ?? false),
  certificate_filters: z.array(certificateFilterSchema).max(20).transform((fs) => fs.filter((f) => Object.values(f).some((v) => v))),
  configuration_ids: z.array(z.string()).optional().transform((v) => v ?? []),
})
export type TargetBaseOutput = z.output<typeof targetSchema>
export type TargetFormOutput = TargetBaseOutput & { overrides: Record<string, Record<string, unknown>> }

/** The id prefix of one attached configuration's override inputs. */
export const overridePrefix = (configurationId: string) => `o-${configurationId}.`

export interface TargetFormContext {
  /** An attached configuration (with its config and target_supplied). */
  configuration: (id: string) => Configuration | undefined
  provider: (type: string) => Provider | undefined
}

/**
 * The target drawer's schema: the target fields plus, for every attached
 * configuration, its override generated from the provider descriptors
 * (overrideToZod: only overridable fields; target-supplied ones required).
 * Override inputs are flat form values "o-<configuration id>.config_overrides.<key>".
 */
export function targetFormSchema(ctx: TargetFormContext) {
  return z.record(z.string(), z.unknown()).transform((values, issues): TargetFormOutput => {
    const base = targetSchema.safeParse(values)
    if (!base.success) for (const i of base.error.issues) issues.addIssue({ ...i } as never)
    const overrides: Record<string, Record<string, unknown>> = {}
    let ok = base.success
    for (const id of (values.configuration_ids as string[] | undefined) ?? []) {
      const c = ctx.configuration(id)
      const p = c && ctx.provider(c.provider_type)
      if (!c || !p) continue
      const prefix = overridePrefix(id)
      const r = overrideToZod(p, c.config ?? {}, { idPrefix: prefix }).safeParse(buildOverride(p, values, prefix))
      if (r.success) overrides[id] = r.data
      else {
        ok = false
        for (const i of r.error.issues) issues.addIssue({ code: 'custom', path: i.path, message: i.message, params: (i as { params?: Record<string, unknown> }).params ?? {} })
      }
    }
    if (!ok || !base.success) return z.NEVER
    return { ...base.data, overrides }
  })
}
