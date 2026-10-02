import { z } from 'zod'
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
})

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
