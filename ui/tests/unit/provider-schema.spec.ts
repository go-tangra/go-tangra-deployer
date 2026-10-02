// fieldsToZod / overrideToZod against the shared vectors the Go validator runs
// (api/testdata/provider-field-vectors.json, internal/provider/schema_test.go):
// same accept/reject, same field paths and codes, same target-supplied keys.
import { describe, expect, expectTypeOf, it } from 'vitest'
import vectorsJSON from '../../../api/testdata/provider-field-vectors.json'
import goldenJSON from '../../../internal/providers/all/testdata/capabilities.golden.json'
import type { components } from '@/api/schema.d'
import type { Provider, ProviderField } from '@/api/types'
import { buildOverride, buildPayload, describeCode, fieldsToZod, initialValues, isAuthHeader, overrideToZod, overrideValues, safeKey, unknownKeys, validateInput, validateLabel, validateOverride } from '@/schemas/providerFields'

interface Case {
  name: string
  provider: string
  mode: 'configuration' | 'effective' | 'override'
  config: Record<string, unknown>
  credentials?: Record<string, unknown>
  override?: Record<string, unknown>
  errors: Record<string, string>
  target_supplied?: string[]
}
const vectors = vectorsJSON as unknown as { providers: Record<string, Provider>; cases: Case[] }
const golden = goldenJSON as unknown as { items: Provider[] }

/** The issues of a failed parse as path -> code. */
function codes(res: { success: boolean; error?: { issues: readonly { path: PropertyKey[]; params?: Record<string, unknown> | undefined }[] } | undefined }): Record<string, string> {
  const out: Record<string, string> = {}
  for (const i of res.error?.issues ?? []) out[i.path.map(String).join('.')] = String(i.params?.code)
  return out
}

describe('shared validator vectors', () => {
  for (const c of vectors.cases) {
    it(c.name, () => {
      const p = vectors.providers[c.provider]!
      if (c.mode === 'override') {
        expect(validateOverride(p, c.config, c.override ?? {})).toEqual(c.errors)
        const res = overrideToZod(p, c.config).safeParse(c.override ?? {})
        expect(res.success).toBe(Object.keys(c.errors).length === 0)
        if (!res.success) expect(codes(res)).toEqual(c.errors)
        return
      }
      const direct = validateInput(p, c.config, c.credentials ?? null, c.mode)
      expect(direct.errors).toEqual(c.errors)
      expect(direct.targetSupplied).toEqual(c.target_supplied ?? [])
      const res = fieldsToZod(p, { mode: c.mode }).safeParse({ config: c.config, credentials: c.credentials ?? {} })
      expect(res.success).toBe(Object.keys(c.errors).length === 0)
      if (res.success) expect(res.data.target_supplied).toEqual(c.target_supplied ?? [])
      else expect(codes(res)).toEqual(c.errors)
    })
  }
})

describe('descriptor helpers', () => {
  const byType = Object.fromEntries(golden.items.map((p) => [p.type, p]))

  it('generated OpenAPI types are assignable to the UI descriptor types', () => {
    expectTypeOf<components['schemas']['ProviderField']>().toExtend<ProviderField>()
    expectTypeOf<components['schemas']['ProviderCapabilities']>().toExtend<Provider>()
  })

  it('validate action per capability: Test connection, Check settings, Preview hosts', () => {
    expect(validateLabel(byType.bigip!)).toBe('Test connection')
    expect(validateLabel(byType.webhook!)).toBe('Test connection')
    expect(validateLabel(byType.cloudflare!)).toBe('Check settings')
    expect(validateLabel(byType.aws_acm!)).toBe('Check settings')
    expect(validateLabel(byType['inventory-agent']!)).toBe('Preview hosts')
  })

  it('create defaults, edit values (secrets never), payload of declared non-empty keys only', () => {
    const big = byType.bigip!
    expect(initialValues(big)).toEqual({ 'config.partition': 'Common', 'credentials.host': '', 'credentials.username': '', 'credentials.password': '' })
    const edit = initialValues(big, { config: { partition: 'Prod', legacy: 1 }, credentials_public: { host: 'bigip.example', username: 'ops', password: 'never' } })
    expect(edit).toEqual({ 'config.partition': 'Prod', 'credentials.host': 'bigip.example', 'credentials.username': 'ops', 'credentials.password': '' })
    expect(buildPayload(big, { ...edit, 'config.legacy': 'x' })).toEqual({ config: { partition: 'Prod' }, credentials: { host: 'bigip.example', username: 'ops' } })
    expect(unknownKeys(big, { partition: 'P', endpoint: 'x', zeta: 1 })).toEqual(['endpoint', 'zeta'])
    const inv = byType['inventory-agent']!
    const iv = initialValues(inv)
    expect(iv['config.wait_seconds']).toBe(60)
    expect(buildPayload(inv, { ...iv, 'config.host_tags': [' role=web ', ''] }).config).toEqual({ host_tags: ['role=web'], key_policy: 'require', require_all_success: false, wait_seconds: 60 })
  })

  it('override values: three-state booleans, empty values inherit', () => {
    const inv = byType['inventory-agent']!
    const v = overrideValues(inv, { require_all_success: true, cert_name: 'www' }, 'o.')
    expect(v['o.config_overrides.require_all_success']).toBe('true')
    expect(v['o.config_overrides.wait_seconds']).toBe('')
    expect(buildOverride(inv, { ...v, 'o.config_overrides.host_tags': ['a=b'] }, 'o.')).toEqual({ require_all_success: true, cert_name: 'www', host_tags: ['a=b'] })
  })

  it('wording comes from the descriptor, never from a value', () => {
    const inv = byType['inventory-agent']!
    expect(describeCode('one_of_required:host_ids,host_tags', inv, 'host_ids')).toMatch(/select hosts or enter host tags/i)
    expect(describeCode('out_of_range:0..240')).toBe('Must be between 0 and 240.')
    expect(describeCode('pattern', byType.cloudflare!, 'zone_id')).toContain('023e105f4ecef8ad9ca31a8372d0c353')
    expect(describeCode('too_long:32')).toBe('Must be at most 32 characters.')
    expect(isAuthHeader(' X-Webhook-Secret ')).toBe(true)
    expect(isAuthHeader('X-Trace')).toBe(false)
    expect(safeKey('a b/ü')).toBe('a_b___')
  })
})
