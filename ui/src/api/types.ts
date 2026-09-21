// Domain types mirror the deployer OpenAPI responses (see api/openapi/deployer.yaml).

export type ConfigStatus = 'active' | 'inactive' | 'error'
export type JobStatus = 'pending' | 'processing' | 'completed' | 'failed' | 'cancelled' | 'retrying' | 'partial'
export type JobType = 'parent' | 'child' | 'direct'
export type TriggeredBy = 'manual' | 'event' | 'auto_renewal'
export type HistoryAction = 'deploy' | 'verify' | 'rollback'
export type HistoryResult = 'success' | 'failure' | 'partial'

// A provider's declared capabilities (drives the configuration form).
export interface Provider {
  type: string
  display_name: string
  supports_verify: boolean
  supports_rollback: boolean
  required_config?: string[]
  required_credentials?: string[]
}

export interface Configuration {
  id: string
  name: string
  description?: string | undefined
  provider_type: string
  config?: Record<string, unknown>
  status: ConfigStatus
  status_message?: string
  has_credentials: boolean
  last_deployment_at?: string
  created_at?: string
  updated_at?: string
}

export interface ConfigurationInput {
  name: string
  description?: string | undefined
  provider_type: string
  config?: Record<string, unknown>
  credentials?: Record<string, unknown>
}

export interface CertificateFilter {
  issuer?: string
  common_name?: string
  san?: string
  organization?: string
  organizational_unit?: string
  country?: string
}

export interface Target {
  id: string
  name: string
  description?: string | undefined
  auto_deploy: boolean
  certificate_filters: CertificateFilter[]
  configuration_ids: string[]
  created_at?: string
  updated_at?: string
}

export interface TargetInput {
  name: string
  description?: string | undefined
  auto_deploy: boolean
  certificate_filters: CertificateFilter[]
}

export interface Job {
  id: string
  type: JobType
  deployment_target_id?: string
  target_configuration_id?: string
  parent_job_id?: string
  certificate_id: string
  certificate_serial?: string
  status: JobStatus
  status_message?: string
  progress: number
  retry_count: number
  max_retries: number
  triggered_by: TriggeredBy
  created_at: string
  completed_at?: string
}

export interface HistoryEntry {
  action: HistoryAction
  result: HistoryResult
  message?: string
  duration_ms: number
  created_at: string
}

export interface JobResult extends Job {
  result?: unknown
  history: HistoryEntry[]
  children?: Job[]
}

export interface ActionResult {
  action: HistoryAction
  success: boolean
  message?: string
}

// Dashboard statistics (best-effort; unpopulated fields render as zero).
export interface Stats {
  jobs_by_status?: Record<string, number>
  jobs_by_trigger?: Record<string, number>
  targets_total?: number
  configurations_total?: number
  configurations_by_status?: Record<string, number>
  configurations_by_provider?: Record<string, number>
  success_rate_24h?: number
  success_rate_7d?: number
  auto_deploy_targets?: number
  recent_errors?: { job_id: string; message: string; at: string }[]
}
