// Domain types mirror the deployer OpenAPI responses (see api/openapi/deployer.yaml).

export type ConfigStatus = 'active' | 'inactive' | 'error'
export type JobStatus = 'pending' | 'processing' | 'completed' | 'failed' | 'cancelled' | 'retrying' | 'partial'
export type JobType = 'parent' | 'child' | 'direct'
export type TriggeredBy = 'manual' | 'event' | 'auto_renewal'
export type HistoryAction = 'deploy' | 'verify' | 'rollback'
export type HistoryResult = 'success' | 'failure' | 'partial'

export type FieldType = 'string' | 'text' | 'url' | 'int' | 'bool' | 'enum' | 'string_list' | 'key_value' | 'host_selector'
export type FieldGroup = 'connection' | 'credentials' | 'options'

export interface FieldOption {
  value: string
  label: string
}

/** A provider field descriptor (contracts/deployer-config-ui.md §2); optional keys are omitted when unset. */
export interface ProviderField {
  key: string
  label: string
  /** Absent = string. */
  type?: FieldType
  secret?: boolean
  required?: boolean
  overridable?: boolean
  default?: unknown
  options?: FieldOption[]
  help?: string
  placeholder?: string
  group?: FieldGroup
  min?: number
  max?: number
  max_length?: number
  pattern?: string
  max_items?: number
}

/** A provider's declared capabilities: the single source of truth for the configuration form. */
export interface Provider {
  type: string
  display_name: string
  description?: string
  supports_verify: boolean
  supports_rollback: boolean
  delivers_by_reference?: boolean
  /** Absent = false (validate only checks the input). */
  test_connection?: boolean
  schema_version?: number
  config_fields: ProviderField[] | null
  credential_fields: ProviderField[] | null
  /** Absent = []. */
  one_of_required?: string[][]
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
  /** Required keys left empty for the deployment targets to supply. */
  target_supplied?: string[]
  /** Single reads by managers: names of the stored credential fields. */
  credentials_set?: string[]
  /** Single reads by managers: values of the non-secret credential fields. */
  credentials_public?: Record<string, unknown>
  ignored_config_keys?: string[]
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
  clear_credentials?: string[]
}

/** POST /configurations/validate result. */
export interface ValidateResult {
  valid: boolean
  checked: 'probe' | 'static' | 'partial'
  deferred?: string[]
  details?: Record<string, unknown>
}

/** inventory-agent preview: one matched host. */
export interface MatchedHost {
  host_id: string
  hostname: string
  os_name?: string
  tags?: Record<string, string>
  agent_online?: boolean
  capability?: string
}

/** inventory-agent job result: one host's delivery state. */
export interface HostResult {
  host_id: string
  hostname?: string
  state: string
  agent_online?: boolean
  attempts?: number
  hook_exit_code?: number
  reason?: string
  serial?: string
  fingerprint?: string
}

export interface DeliveryCounts {
  installed?: number
  unchanged?: number
  queued?: number
  failed?: number
  unsupported?: number
  superseded?: number
  total?: number
}

/** A target's certificate filter as the API stores it (Go store.CertificateFilter). */
export interface CertificateFilter {
  issuer_name?: string
  common_name_pattern?: string
  san_pattern?: string
  subject_organization?: string
  subject_org_unit?: string
  subject_country?: string
}

export interface Target {
  id: string
  name: string
  description?: string | undefined
  auto_deploy: boolean
  certificate_filters: CertificateFilter[]
  configuration_ids: string[]
  /** Per attached configuration (id -> override): overridable provider config only. */
  config_overrides?: Record<string, Record<string, unknown>>
  /** Single reads: per attached configuration, labels of required fields the merged configuration lacks. */
  missing_required?: Record<string, string[]>
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
  /** The cause of the last failure (provider or lcm error); cleared by a success. */
  error?: string
  progress: number
  retry_count: number
  max_retries: number
  triggered_by: TriggeredBy
  created_at: string
  completed_at?: string
}

export interface HistoryEntry {
  id?: string
  action: HistoryAction
  result: HistoryResult
  message?: string
  duration_ms: number
  created_at: string
}

export interface JobResult extends Job {
  result?: { message?: string; error?: string; details?: Record<string, unknown> } | null
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
