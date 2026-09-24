import { z } from 'zod'
import { nonEmpty, optionalString, jsonObject } from '@go-tangra/ui/forms'

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

/** POST/PUT /targets payload plus the attachment reconciliation inputs. */
export const targetSchema = z.object({
  name: nonEmpty(200),
  description: optionalString(2000),
  auto_deploy: z.boolean().optional().transform((v) => v ?? false),
  certificate_filters: z.array(certificateFilterSchema).max(20).transform((fs) => fs.filter((f) => Object.values(f).some((v) => v))),
  configuration_ids: z.array(z.string()).optional().transform((v) => v ?? []),
  overrides: jsonObject.pipe(z.record(z.string(), z.record(z.string(), z.unknown()))),
})
export type TargetFormOutput = z.output<typeof targetSchema>
