import { z } from 'zod'

export const JOB_STATUSES = ['pending', 'processing', 'completed', 'failed', 'partial', 'retrying', 'cancelled'] as const
export const JOB_TYPES = ['parent', 'child', 'direct'] as const

export const jobFilterSchema = z.object({
  status: z.enum(JOB_STATUSES).optional(),
  job_type: z.enum(JOB_TYPES).optional(),
})
