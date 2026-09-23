import { z } from 'zod'
import { nonEmpty, optionalString, jsonObject } from '@freya/ui/forms'

/** POST/PUT /configurations payload. Credentials are write-only: sent when given, never echoed. */
export const configurationSchema = z.object({
  name: nonEmpty(200),
  description: optionalString(2000),
  provider_type: nonEmpty(64),
  config: jsonObject,
  credentials: jsonObject,
})
export type ConfigurationFormOutput = z.output<typeof configurationSchema>
