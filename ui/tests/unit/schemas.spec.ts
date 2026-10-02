import { describe, expect, it } from 'vitest'
import { certificateFilterSchema, configurationSchema, jobFilterSchema, targetFormSchema, targetSchema } from '@/schemas'
import type { Configuration, Provider } from '@/api/types'

const cloudflare: Provider = {
  type: 'cloudflare', display_name: 'Cloudflare', supports_verify: true, supports_rollback: false,
  config_fields: [{ key: 'zone_id', label: 'Zone ID', type: 'string', required: true, overridable: true, group: 'connection', pattern: '^[a-fA-F0-9]{32}$', max_length: 32 }],
  credential_fields: [{ key: 'api_token', label: 'API token', type: 'string', secret: true, required: true, group: 'credentials', max_length: 16 }],
}
const ctx = (over: Partial<Parameters<typeof configurationSchema>[0]> = {}) => configurationSchema({ provider: () => cloudflare, credentialsSet: () => [], cleared: () => [], ...over })

// T035/T092: deployer write schemas; credential/secret values never appear in an
// issue message (they are write-only; codes are built from the descriptor).
describe('deployer schemas', () => {
  it('configuration: base fields + descriptor schema over flat values → request payload', () => {
    const r = ctx().parse({ name: ' Edge ', description: '', provider_type: 'cloudflare', 'config.zone_id': '023e105f4ecef8ad9ca31a8372d0c353', 'credentials.api_token': 'tok' })
    expect(r).toEqual({ name: 'Edge', description: undefined, provider_type: 'cloudflare', config: { zone_id: '023e105f4ecef8ad9ca31a8372d0c353' }, credentials: { api_token: 'tok' }, target_supplied: [] })
    // Zone ID is overridable: empty → target-supplied, not an error.
    expect(ctx().parse({ name: 'x', provider_type: 'cloudflare', 'credentials.api_token': 'tok' }).target_supplied).toEqual(['zone_id'])
    // No provider chosen → refused.
    expect(ctx({ provider: () => undefined }).safeParse({ name: 'x', provider_type: '' }).success).toBe(false)
    // Edit: a blank stored secret is kept (left out), clear_credentials sent.
    const e = ctx({ credentialsSet: () => ['api_token'] }).parse({ name: 'x', provider_type: 'cloudflare', 'credentials.api_token': '' })
    expect(e.credentials).toBeUndefined()
  })
  it('secret values are never echoed in error text', () => {
    const secret = 'hunter2-super-secret-token'
    const r = ctx().safeParse({ name: 'x', provider_type: 'cloudflare', 'config.zone_id': secret, 'credentials.api_token': secret })
    expect(r.success).toBe(false)
    if (r.success) return
    expect(r.error.issues.map((i) => i.path.join('.')).sort()).toEqual(['config.zone_id', 'credentials.api_token'])
    expect(JSON.stringify(r.error.issues)).not.toContain(secret)
  })
  it('target: filters are regex-validated and blank rules dropped; overrides validated per configuration', () => {
    expect(certificateFilterSchema.safeParse({ common_name: '(' }).success).toBe(false)
    expect(certificateFilterSchema.safeParse({ common_name: '^api\\.' }).success).toBe(true)
    const t = targetSchema.parse({ name: 'Prod', certificate_filters: [{ issuer: '', common_name: '', san: '', organization: '' }, { common_name: 'example' }] })
    expect(t.certificate_filters).toEqual([{ issuer: undefined, common_name: 'example', san: undefined, organization: undefined }])
    expect(targetSchema.safeParse({ name: 'Prod', certificate_filters: new Array(21).fill({}) }).success).toBe(false)
    const cfg: Configuration = { id: 'c1', name: 'CF', provider_type: 'cloudflare', status: 'active', has_credentials: true, config: {}, target_supplied: ['zone_id'] }
    const s = targetFormSchema({ configuration: (id) => (id === 'c1' ? cfg : undefined), provider: () => cloudflare })
    const bad = s.safeParse({ name: 'T', certificate_filters: [], configuration_ids: ['c1'], 'o-c1.config_overrides.zone_id': '' })
    expect(bad.success ? [] : bad.error.issues.map((i) => i.path.join('.'))).toEqual(['o-c1.config_overrides.zone_id'])
    const ok = s.parse({ name: 'T', certificate_filters: [], configuration_ids: ['c1'], 'o-c1.config_overrides.zone_id': '023e105f4ecef8ad9ca31a8372d0c353' })
    expect(ok.overrides).toEqual({ c1: { zone_id: '023e105f4ecef8ad9ca31a8372d0c353' } })
  })
  it('job filter: only known statuses and types', () => {
    expect(jobFilterSchema.safeParse({ status: 'exploded' }).success).toBe(false)
    expect(jobFilterSchema.safeParse({ status: 'failed', job_type: 'child' }).success).toBe(true)
  })
})
