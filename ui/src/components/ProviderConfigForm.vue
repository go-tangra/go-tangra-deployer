<script setup lang="ts">
// The provider fields of a configuration (or of a target override), rendered
// from the provider's descriptors (contracts/deployer-config-ui.md §6): one
// section per group (Connection, Credentials, Options — empty ones hidden,
// Options collapsed at its defaults), a kit input per descriptor type, help
// and placeholder from the descriptor, required markers, write-only secrets.
//
// Values and errors are the owner's flat form state keyed by input id:
// `${idPrefix}config.<key>`, `${idPrefix}credentials.<key>` or, in override
// mode, `${idPrefix}config_overrides.<key>`. A `host_selector` field renders
// through the slot `field-<key>` (HostPicker) with a manual list fallback.
import { computed } from 'vue'
import { UiBadge, UiButton, UiIcon, UiInput, UiKeyValueTable, UiSecretField, UiSection, UiSelect, UiCombobox, UiSwitch, UiTagEditor, UiTextarea, type SelectOption } from '@go-tangra/ui'
import StringListInput from '@/components/StringListInput.vue'
import type { FieldGroup, Provider, ProviderField } from '@/api/types'
import { configFields, credentialFields, fieldType, isEmpty, oneOfRequired, overridableFields, PATH_CONFIG, PATH_CREDENTIALS, PATH_OVERRIDES } from '@/schemas/providerFields'

const props = withDefaults(defineProps<{
  provider: Provider
  values: Record<string, unknown>
  errors?: Record<string, string> | undefined
  /** configuration: the configuration drawer; override: a target's override; view: read-only. */
  mode?: 'configuration' | 'override' | 'view' | undefined
  idPrefix?: string | undefined
  /** Editing a stored configuration (secrets are write-only). */
  editing?: boolean | undefined
  /** Stored credential names (edit). */
  credentialsSet?: readonly string[] | undefined
  /** Optional credentials marked for removal (edit). */
  cleared?: readonly string[] | undefined
  /** Keys the stored configuration leaves to the targets. */
  targetSupplied?: readonly string[] | undefined
  /** Override mode: the configuration's own values (shown as "Inherited: …"). */
  inherited?: Record<string, unknown> | undefined
  /** Whether the Options section starts open. */
  optionsOpen?: boolean | undefined
  disabled?: boolean | undefined
}>(), { mode: 'configuration', idPrefix: '', editing: false, optionsOpen: false, disabled: false, errors: () => ({}), credentialsSet: () => [], cleared: () => [], targetSupplied: () => [], inherited: () => ({}) })
const emit = defineEmits<{
  (e: 'update', id: string, value: unknown): void
  (e: 'blur', id: string): void
  (e: 'clear', key: string, on: boolean): void
  (e: 'options-toggle', open: boolean): void
}>()

interface Row {
  f: ProviderField
  credential: boolean
  id: string
  path: string
}

const groupOf = (f: ProviderField, credential: boolean): FieldGroup => f.group ?? (credential ? 'credentials' : 'connection')

const rows = computed<Row[]>(() => {
  if (props.mode === 'override') return overridableFields(props.provider).map((f) => ({ f, credential: false, path: PATH_OVERRIDES + f.key, id: props.idPrefix + PATH_OVERRIDES + f.key }))
  const cfg = configFields(props.provider).map((f) => ({ f, credential: false, path: PATH_CONFIG + f.key, id: props.idPrefix + PATH_CONFIG + f.key }))
  const cred = credentialFields(props.provider).map((f) => ({ f, credential: true, path: PATH_CREDENTIALS + f.key, id: props.idPrefix + PATH_CREDENTIALS + f.key }))
  return [...cfg, ...cred]
})
const SECTIONS: { group: FieldGroup; title: string }[] = [
  { group: 'connection', title: 'Connection' },
  { group: 'credentials', title: 'Credentials' },
  { group: 'options', title: 'Options' },
]
const sections = computed(() => SECTIONS.map((s) => ({ ...s, rows: rows.value.filter((r) => groupOf(r.f, r.credential) === s.group) })).filter((s) => s.rows.length))
/** Options collapse in the configuration drawer (open when not at the defaults). */
const collapsible = (g: FieldGroup) => g === 'options' && props.mode === 'configuration'
const onToggle = (e: Event) => {
  if (e.target instanceof HTMLDetailsElement) emit('options-toggle', e.target.open)
}

const val = (r: Row) => props.values[r.id]
const err = (r: Row) => props.errors[r.id]
const set = (r: Row, v: unknown) => emit('update', r.id, v)

// --- one_of_required groups -------------------------------------------------
const groupFor = (key: string) => oneOfRequired(props.provider).find((g) => g.includes(key))
const labelOf = (key: string) => configFields(props.provider).find((f) => f.key === key)?.label ?? key
const groupOverridable = (g: string[]) => g.every((k) => configFields(props.provider).find((f) => f.key === k)?.overridable)

// --- configuration mode -----------------------------------------------------
const stored = (r: Row) => r.credential && props.editing && props.credentialsSet.includes(r.f.key)
const isCleared = (r: Row) => props.cleared.includes(r.f.key)
/** Required in the browser (marker + aria): required and not left to the targets. */
function required(r: Row): boolean {
  if (props.mode === 'override') return props.targetSupplied.includes(r.f.key)
  if (!r.f.required) return false
  if (r.f.overridable) return false
  return !stored(r) || isCleared(r)
}
function hint(r: Row): string | undefined {
  const help = r.f.help
  const join = (...p: (string | undefined)[]) => p.filter(Boolean).join(' ') || undefined
  if (props.mode === 'override') {
    if (props.targetSupplied.includes(r.f.key)) {
      const g = groupFor(r.f.key)
      return join(g ? `Required — the configuration leaves ${g.map(labelOf).join(' or ')} to each target.` : 'Required — the configuration leaves it to each target.', help)
    }
    return join(help, inheritedHint(r))
  }
  if (r.f.secret && stored(r)) return isCleared(r) ? 'Removed when you save.' : join('Stored — leave blank to keep.', help)
  const g = !r.credential ? groupFor(r.f.key) : undefined
  if (g) {
    const empty = g.every((k) => isEmpty(props.values[props.idPrefix + PATH_CONFIG + k]))
    if (empty && groupOverridable(g) && props.targetSupplied.includes(r.f.key)) return join('To be provided by each target.', help)
    // The rule of the group is told once, under its last member.
    if (g[g.length - 1] !== r.f.key) return help
    const names = g.map(labelOf).join(' or ')
    return join(help, groupOverridable(g) ? `Fill in ${names} — or leave both empty for each target to provide.` : `Fill in ${names}.`)
  }
  if (r.f.required && r.f.overridable) {
    if (isEmpty(val(r)) && props.targetSupplied.includes(r.f.key)) return join('To be provided by each target.', help)
    return join('Required — or leave empty and let each target provide it.', help)
  }
  return help
}

// --- override mode ----------------------------------------------------------
function display(f: ProviderField, v: unknown): string {
  if (v === undefined || v === null || v === '') return ''
  if (fieldType(f) === 'bool') return v ? 'on' : 'off'
  if (fieldType(f) === 'enum') return f.options?.find((o) => o.value === v)?.label ?? String(v)
  if (Array.isArray(v)) return v.join(', ')
  if (typeof v === 'object') return Object.entries(v as Record<string, unknown>).map(([k, x]) => (x ? `${k}=${String(x)}` : k)).join(', ')
  return String(v)
}
const inheritedValue = (r: Row) => {
  const v = props.inherited[r.f.key]
  return isEmpty(v) && fieldType(r.f) !== 'bool' ? (r.f.default !== undefined ? display(r.f, r.f.default) : '') : display(r.f, v ?? r.f.default)
}
function placeholder(r: Row): string | undefined {
  if (props.mode === 'configuration' && !r.credential && props.targetSupplied.includes(r.f.key) && isEmpty(val(r))) return 'Provided by each target'
  if (props.mode !== 'override') return r.f.placeholder
  const inh = inheritedValue(r)
  return inh ? 'Inherited: ' + inh : r.f.placeholder
}
/** key_value / host fields have no placeholder: the inherited value goes into the hint. */
function inheritedHint(r: Row): string | undefined {
  const t = fieldType(r.f)
  if (t !== 'key_value') return undefined
  const inh = inheritedValue(r)
  return inh ? 'Inherited: ' + inh + '.' : undefined
}
const enumOptions = (f: ProviderField): SelectOption[] => (f.options ?? []).map((o) => ({ title: o.label, value: o.value }))
const boolOptions: SelectOption[] = [{ title: 'On', value: 'true' }, { title: 'Off', value: 'false' }]

// --- view mode --------------------------------------------------------------
function viewItems(rs: Row[]) {
  return rs.map((r) => ({ label: r.f.label, value: r.f.secret ? '' : display(r.f, val(r)) }))
}
const secretStored = (r: Row) => props.credentialsSet.includes(r.f.key)
</script>

<template>
  <div class="flex flex-col gap-6" :data-provider="provider.type">
    <template v-if="mode === 'view'">
      <UiSection v-for="s in sections" :key="s.group" :title="s.title">
        <UiKeyValueTable :items="viewItems(s.rows)">
          <template v-for="(r, i) in s.rows" :key="r.id" #[`value-${i}`]="{ item }">
            <UiBadge v-if="r.f.secret" :color="secretStored(r) ? 'success' : 'neutral'" data-test="secret-stored">{{ secretStored(r) ? 'stored' : 'not set' }}</UiBadge>
            <span v-else>{{ item.value || '—' }}</span>
          </template>
        </UiKeyValueTable>
      </UiSection>
    </template>
    <template v-else>
      <component :is="collapsible(s.group) || mode === 'override' ? 'div' : UiSection" v-for="s in sections" :key="s.group" v-bind="collapsible(s.group) || mode === 'override' ? {} : { title: s.title }" :data-section="s.group">
        <component :is="collapsible(s.group) ? 'details' : 'div'" :class="collapsible(s.group) ? 'group rounded-box border border-base-300' : 'flex flex-col gap-4'" :open="collapsible(s.group) ? optionsOpen || undefined : undefined" @toggle="onToggle">
          <summary v-if="collapsible(s.group)" class="flex cursor-pointer list-none items-center justify-between gap-2 rounded-box px-4 py-3 hover:bg-base-200 [&::-webkit-details-marker]:hidden" data-test="options-toggle">
            <h2 class="text-base font-semibold">{{ s.title }}</h2>
            <UiIcon name="mdi-chevron-down" size="sm" class="transition-transform group-open:rotate-180" />
          </summary>
          <div :class="collapsible(s.group) ? 'flex flex-col gap-4 px-4 pt-1 pb-4' : 'contents'">
            <div v-for="r in s.rows" :key="r.id" :data-field="r.path" class="min-w-0">
              <slot :name="'field-' + r.f.key" v-bind="{ id: r.id, path: r.path, field: r.f, value: val(r), error: err(r), required: required(r), hint: hint(r), update: (v: unknown) => set(r, v), blur: () => emit('blur', r.id) }">
                <div v-if="r.f.secret" class="flex items-start gap-2">
                  <div class="min-w-0 grow">
                    <UiSecretField :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :disabled="disabled || isCleared(r)" :model-value="val(r)" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                  </div>
                  <UiButton v-if="stored(r) && !r.f.required" size="sm" variant="soft" :color="isCleared(r) ? 'neutral' : 'error'" class="mt-6" :data-test="'clear-' + r.f.key" @click="emit('clear', r.f.key, !isCleared(r))">{{ isCleared(r) ? 'Keep' : 'Clear' }}</UiButton>
                </div>
                <UiSwitch v-else-if="fieldType(r.f) === 'bool' && mode !== 'override'" :id="r.id" :label="r.f.label" :hint="hint(r)" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" />
                <UiSelect v-else-if="fieldType(r.f) === 'bool'" :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :options="boolOptions" :placeholder="placeholder(r) ?? 'Inherited'" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <UiInput v-else-if="fieldType(r.f) === 'int'" :id="r.id" type="number" inputmode="numeric" :step="1" :min="r.f.min" :max="r.f.max" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :placeholder="placeholder(r)" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <UiSelect v-else-if="fieldType(r.f) === 'enum' && ((r.f.options?.length ?? 0) <= 12 || mode === 'override')" :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :options="enumOptions(r.f)" :placeholder="mode === 'override' ? placeholder(r) ?? 'Inherited' : undefined" :clearable="mode === 'override' || (!r.f.required && r.f.default === undefined)" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <UiCombobox v-else-if="fieldType(r.f) === 'enum'" :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :options="enumOptions(r.f)" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <UiTextarea v-else-if="fieldType(r.f) === 'text'" :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :placeholder="placeholder(r)" :rows="3" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <StringListInput v-else-if="fieldType(r.f) === 'string_list' || fieldType(r.f) === 'host_selector'" :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :placeholder="placeholder(r)" :max="r.f.max_items" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <UiTagEditor v-else-if="fieldType(r.f) === 'key_value'" :id="r.id" :label="r.f.label" :hint="hint(r)" :error="err(r)" :max="r.f.max_items" :model-value="val(r)" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
                <UiInput v-else :id="r.id" :type="fieldType(r.f) === 'url' ? 'url' : 'text'" :inputmode="fieldType(r.f) === 'url' ? 'url' : undefined" autocomplete="off" :label="r.f.label" :hint="hint(r)" :error="err(r)" :required="required(r)" :placeholder="placeholder(r)" :model-value="val(r)" :disabled="disabled" @update:model-value="set(r, $event)" @blur="emit('blur', r.id)" />
              </slot>
            </div>
          </div>
        </component>
      </component>
    </template>
  </div>
</template>
