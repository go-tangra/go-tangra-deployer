import { describe, expect, it } from 'vitest'
import { certificateFilterSchema, configurationSchema, jobFilterSchema, targetSchema } from '@/schemas'

// T035: deployer write schemas; credential/secret values never appear in an
// issue message (they are write-only and the JSON grammar error is generic).
describe('deployer schemas', () => {
  it('configuration: provider + JSON config/credentials parsed to objects; blank → {}', () => {
    const r = configurationSchema.parse({ name: ' Edge ', description: '', provider_type: 'ssh', config: '{"host":"edge.example.org"}', credentials: '' })
    expect(r).toEqual({ name: 'Edge', description: undefined, provider_type: 'ssh', config: { host: 'edge.example.org' }, credentials: {} })
    expect(configurationSchema.safeParse({ name: 'x', provider_type: '', config: '{}', credentials: '{}' }).success).toBe(false)
  })
  it('secret refs and credential values are never echoed in error text', () => {
    const secret = 'hunter2-super-secret-token'
    const r = configurationSchema.safeParse({ name: 'x', provider_type: 'ssh', config: '{}', credentials: `{"password": "${secret}"` })
    expect(r.success).toBe(false)
    if (r.success) return
    for (const issue of r.error.issues) expect(issue.message).not.toContain(secret)
    expect(JSON.stringify(r.error.issues)).not.toContain(secret)
    const arr = configurationSchema.safeParse({ name: 'x', provider_type: 'ssh', config: '{}', credentials: `["${secret}"]` })
    expect(arr.success).toBe(false)
    if (!arr.success) expect(JSON.stringify(arr.error.issues)).not.toContain(secret)
  })
  it('target: filters are regex-validated and blank rules dropped; overrides keyed by configuration id', () => {
    expect(certificateFilterSchema.safeParse({ common_name: '(' }).success).toBe(false)
    expect(certificateFilterSchema.safeParse({ common_name: '^api\\.' }).success).toBe(true)
    const t = targetSchema.parse({ name: 'Prod', certificate_filters: [{ issuer: '', common_name: '', san: '', organization: '' }, { common_name: 'example' }], overrides: '{"c1": {"path": "/etc/ssl"}}' })
    expect(t.certificate_filters).toEqual([{ issuer: undefined, common_name: 'example', san: undefined, organization: undefined }])
    expect(t).toMatchObject({ auto_deploy: false, configuration_ids: [], overrides: { c1: { path: '/etc/ssl' } } })
    expect(targetSchema.safeParse({ name: 'Prod', certificate_filters: [], overrides: '{"c1": "not-an-object"}' }).success).toBe(false)
    expect(targetSchema.safeParse({ name: 'Prod', certificate_filters: new Array(21).fill({}), overrides: '' }).success).toBe(false)
  })
  it('job filter: only known statuses and types', () => {
    expect(jobFilterSchema.safeParse({ status: 'exploded' }).success).toBe(false)
    expect(jobFilterSchema.safeParse({ status: 'failed', job_type: 'child' }).success).toBe(true)
  })
})
