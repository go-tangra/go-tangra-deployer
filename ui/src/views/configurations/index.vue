<script setup lang="ts">
// Target configurations: a server-paged list and the schema-driven drawer
// (feature 033, contracts/deployer-config-ui.md §6). The provider is chosen
// first; its descriptors (GET /providers) drive the fields, their grouping,
// inputs, validation (fieldsToZod) and the validate action. Credentials are
// write-only: secrets are never pre-filled, blank keeps the stored value.
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { UiPage, UiAlert, UiCard, UiButton, UiDataTable, UiStatusChip, UiBadge, UiIcon, UiDrawer, UiDialog, UiForm, UiInput, UiSelect, UiTooltip, UiSkeleton, useConfirm, useListQuery, useToast, type Column, type SelectOption } from '@go-tangra/ui'
import { nonEmpty, useZodForm } from '@go-tangra/ui/forms'
import { z } from 'zod'
import { CONFIG_LIST, useConfigurations } from '@/stores/configurations'
import { useProviders } from '@/stores/providers'
import { ApiError, describe } from '@/api/client'
import { configurationSchema, describeFieldErrors } from '@/schemas'
import { buildPayload, configFields, credentialFields, initialValues, labels, optionsAtDefaults, targetSupplied, unknownKeys, validateInput, validateLabel, PATH_CONFIG, PATH_CREDENTIALS } from '@/schemas/providerFields'
import ProviderConfigForm from '@/components/ProviderConfigForm.vue'
import HostPicker from '@/components/HostPicker.vue'
import { CAPABILITY } from '@/api/inventory'
import type { Configuration, MatchedHost, Provider, ValidateResult } from '@/api/types'

const store = useConfigurations()
const providers = useProviders()
const confirm = useConfirm()
const toast = useToast()
const drawer = ref(false)
const selected = ref<Configuration | null>(null)
const detail = ref<Configuration | null>(null)
const loadingDetail = ref(false)
const error = ref('')
// Server paging and sorting (page / size / sort in the URL: ?configs.page=…).
const lq = useListQuery('configs', CONFIG_LIST)
async function load(): Promise<void> {
  const res = await store.list({}, lq.query.value)
  if (res?.page) lq.clampTo(res.page) // a page beyond the end answers the last page
}
watch(lq.query, () => void load())
onMounted(() => {
  void load()
  void providers.list()
})
const providerOptions = computed<SelectOption[]>(() => providers.items.map((p) => ({ title: p.display_name, value: p.type })))
const provider = computed<Provider | undefined>(() => providers.get(String(form.values.provider_type ?? '')))

// --- the drawer form ---------------------------------------------------------
const credentialsSet = ref<string[]>([])
const cleared = ref<string[]>([])
const optionsOpen = ref(false)
/** Readers (no configurations:manage) see the stored values read-only. */
const readOnly = computed(() => !!detail.value && detail.value.has_credentials && detail.value.credentials_set === undefined)
const schema = configurationSchema({ provider: () => provider.value, credentialsSet: () => credentialsSet.value, cleared: () => cleared.value })
const form = useZodForm(schema, {
  onSubmit: async (v) => {
    const { target_supplied: supplied, ...input } = v
    try {
      if (selected.value) await store.update(selected.value.id, input)
      else await store.create(input)
    } catch (e) {
      await explain(e)
      throw e
    }
    return supplied
  },
  onSuccess: (supplied) => {
    drawer.value = false
    if (supplied.length) toast.warning(`Saved. Every target using it must supply ${labels(provider.value, supplied).join(', ')}.`)
    else toast.success('Configuration saved.')
    void load()
  },
})

/** Server field refusals: worded from the descriptor; Options opened when they concern it. */
async function explain(e: unknown): Promise<void> {
  if (!(e instanceof ApiError) || e.reason !== 'validation_failed') return
  const d = e.detail as { fields?: Record<string, unknown>; targets?: { name?: string }[] } | undefined
  if (!d?.fields) return
  d.fields = describeFieldErrors(d.fields, provider.value, d.targets)
  const inOptions = new Set((provider.value ? configFields(provider.value) : []).filter((f) => f.group === 'options').map((f) => PATH_CONFIG + f.key))
  if (Object.keys(d.fields).some((k) => inOptions.has(k))) {
    optionsOpen.value = true
    await nextTick()
  }
}

async function open(c: Configuration | null): Promise<void> {
  selected.value = c
  detail.value = null
  error.value = ''
  credentialsSet.value = []
  cleared.value = []
  optionsOpen.value = false
  validated.value = null
  validateError.value = ''
  providerKey.value++
  form.reset({ name: c?.name ?? '', description: c?.description ?? '', provider_type: c?.provider_type ?? '' })
  drawer.value = true
  if (!c) return
  loadingDetail.value = true
  try {
    // The single read carries credentials_set / credentials_public (managers).
    detail.value = await store.get(c.id)
  } catch (e) {
    error.value = describe(e)
    detail.value = c
  } finally {
    loadingDetail.value = false
  }
  const d = detail.value!
  await providers.list()
  const p = providers.get(d.provider_type)
  credentialsSet.value = [...(d.credentials_set ?? [])]
  form.reset({ name: d.name, description: d.description ?? '', provider_type: d.provider_type, ...(p ? initialValues(p, d) : {}) })
  if (!p) return
  optionsOpen.value = !optionsAtDefaults(p, form.values)
  // A row saved before 033 may lack a required field: highlight it now.
  const declared = Object.fromEntries(Object.entries(d.config ?? {}).filter(([k]) => configFields(p).some((f) => f.key === k)))
  const missing = Object.fromEntries(Object.entries(validateInput(p, declared, null, 'configuration').errors).filter(([, code]) => code === 'required' || code.startsWith('one_of_required')))
  for (const [path, text] of Object.entries(describeFieldErrors(missing, p))) form.setFieldError(path, text)
}

// --- provider choice ---------------------------------------------------------
const providerKey = ref(0)
const providerValues = () => Object.fromEntries(Object.entries(form.values).filter(([k]) => k.startsWith(PATH_CONFIG) || k.startsWith(PATH_CREDENTIALS)))
async function chooseProvider(v: unknown): Promise<void> {
  const next = String(v ?? '')
  const prev = provider.value
  if (next === prev?.type) return
  if (prev && JSON.stringify(providerValues()) !== JSON.stringify(initialValues(prev))) {
    const ok = await confirm.ask({ title: `Discard the values entered for ${prev.display_name}?`, text: 'Switching the provider clears its settings.', confirmLabel: 'Discard', danger: true })
    if (!ok) {
      providerKey.value++ // the select shows the previous provider again
      return
    }
  }
  for (const k of Object.keys(providerValues())) delete form.values[k]
  form.values.provider_type = next
  const p = providers.get(next)
  if (p) Object.assign(form.values, initialValues(p))
  optionsOpen.value = false
  validated.value = null
  validateError.value = ''
  form.errors.value = Object.fromEntries(Object.entries(form.errors.value).filter(([k]) => !k.startsWith(PATH_CONFIG) && !k.startsWith(PATH_CREDENTIALS) && k !== 'provider_type'))
}
const providerHint = computed(() => provider.value?.description ?? 'Choose the provider first; its settings follow.')

function onClear(key: string, on: boolean): void {
  cleared.value = on ? [...new Set([...cleared.value, key])] : cleared.value.filter((k) => k !== key)
  if (on) form.values[PATH_CREDENTIALS + key] = ''
}
const legacyKeys = computed(() => (provider.value && detail.value ? unknownKeys(provider.value, detail.value.config) : []))
/** Required fields currently left to the targets (non-blocking warning). */
const leftToTargets = computed(() => (provider.value ? targetSupplied(provider.value, buildPayload(provider.value, form.values).config) : []))
const secretKeys = computed(() => (provider.value ? credentialFields(provider.value).filter((f) => f.secret).map((f) => f.key) : []))

async function save(): Promise<void> {
  // A refused field inside the collapsed Options section must be focusable.
  if (!form.valid.value && provider.value && !optionsOpen.value) {
    optionsOpen.value = true
    await nextTick()
  }
  await form.submit()
}

// --- Test connection / Check settings / Preview hosts -----------------------
const validated = ref<ValidateResult | null>(null)
const validateError = ref('')
const validating = ref(false)
const actionLabel = computed(() => (provider.value ? validateLabel(provider.value) : 'Check settings'))
async function check(): Promise<void> {
  validated.value = null
  validateError.value = ''
  if (!optionsOpen.value && !form.valid.value) {
    optionsOpen.value = true
    await nextTick()
  }
  const v = form.validate() // the client-side schema first
  if (!v || !provider.value) return
  validating.value = true
  try {
    validated.value = await store.validate({ provider_type: v.provider_type, ...(selected.value ? { configuration_id: selected.value.id } : {}), config: v.config ?? {}, credentials: v.credentials ?? {} })
  } catch (e) {
    await explain(e)
    if (e instanceof ApiError && e.reason === 'validation_failed') form.setServerError(e)
    else validateError.value = describe(e)
  } finally {
    validating.value = false
  }
}
const validatedText = computed(() => {
  const r = validated.value
  if (!r) return ''
  if (r.details?.matched_hosts) {
    const n = matched.value.length
    return `${n} ${n === 1 ? 'host matches' : 'hosts match'} the selection${r.details.truncated ? ' (the first ' + n + ' are listed)' : ''}.`
  }
  if (r.checked === 'partial') return `Checked. Not tested here: ${labels(provider.value, r.deferred ?? []).join(', ')} — each target supplies it.`
  if (r.checked === 'probe') return 'Connection successful.'
  return 'The settings are valid.'
})
type MatchedRow = MatchedHost & Record<string, unknown>
const matched = computed<MatchedRow[]>(() => ((validated.value?.details?.matched_hosts as MatchedHost[] | undefined) ?? []).map((h) => ({ ...h }) as MatchedRow))
const unknownHosts = computed(() => (validated.value?.details?.unknown_host_ids as string[] | undefined) ?? [])
const matchedColumns: Column<MatchedRow>[] = [
  { key: 'hostname', label: 'Host' },
  { key: 'agent_online', label: 'Agent' },
]

async function remove(): Promise<void> {
  if (!selected.value || !(await confirm.ask({ title: `Delete ${selected.value.name}?`, danger: true, confirmLabel: 'Delete' }))) return
  try {
    await store.remove(selected.value.id)
    drawer.value = false
    void load()
  } catch (e) {
    error.value = describe(e)
  }
}

// --- direct deployment ------------------------------------------------------
const deployFor = ref<Configuration | null>(null)
const deployOpen = ref(false)
const deployForm = useZodForm(z.object({ certificate_id: nonEmpty(64) }), {
  onSubmit: async (v) => store.deploy(v.certificate_id, deployFor.value!.id),
  onSuccess: (jobId) => {
    deployOpen.value = false
    toast.success(`Deployment started (job ${jobId}).`)
  },
})
function startDeploy(c: Configuration): void {
  deployFor.value = c
  deployForm.reset({ certificate_id: '' })
  deployOpen.value = true
}
const needs = (c: Configuration) => labels(providers.get(c.provider_type), c.target_supplied ?? [])
const providerName = (t: string) => providers.get(t)?.display_name ?? t

// Only the server's sort fields (CONFIG_LIST) are sortable.
const columns: Column<Configuration>[] = [
  { key: 'name', label: 'Name', sortable: true },
  { key: 'provider_type', label: 'Provider', width: 'sm', sortable: true },
  { key: 'status', label: 'Status', width: 'sm', sortable: true },
  { key: 'has_credentials', label: 'Credentials', width: 'sm', format: (c) => (c.has_credentials ? 'sealed' : '') },
  { key: 'last_deployment_at', label: 'Last deployment', format: (c) => (c.last_deployment_at ? new Date(c.last_deployment_at).toLocaleString() : ''), hideOnStack: true },
  { key: 'created_at', label: 'Created', sortable: true, defaultDir: 'desc', hideOnStack: true, format: (c) => (c.created_at ? new Date(c.created_at).toLocaleString() : '') },
]
</script>

<template>
  <UiPage title="Target configurations">
    <template #actions><UiButton icon="mdi-plus" data-test="config-new" @click="open(null)">New configuration</UiButton></template>
    <UiAlert v-if="store.error" kind="error" class="mb-3">{{ store.error }}</UiAlert>
    <UiCard :padded="false">
      <UiDataTable :items="store.items" :columns="columns" :loading="store.loading" :total="store.total" :page="lq.page.value" :page-size="lq.pageSize.value" :sort="lq.sort.value" caption="Configurations" empty-title="No configurations yet" clickable :row-attrs="(c) => ({ 'data-test': 'config-row-' + c.id })" data-test="configs-table" @row-click="open" @update:page="lq.setPage" @update:page-size="lq.setPageSize" @update:sort="lq.setSort">
        <template #cell-name="{ row }">
          <div class="flex flex-col items-start gap-1">
            <span>{{ row.name }}</span>
            <UiBadge v-if="row.target_supplied?.length" color="warning" data-test="needs-target-values">Needs target values: {{ needs(row).join(', ') }}</UiBadge>
          </div>
        </template>
        <template #cell-provider_type="{ row }"><UiBadge>{{ providerName(row.provider_type) }}</UiBadge></template>
        <template #cell-status="{ row }"><UiStatusChip :status="row.status" :colors="{ inactive: 'neutral' }" /></template>
        <template #cell-has_credentials="{ row }"><UiIcon v-if="row.has_credentials" name="mdi-key" size="sm" class="text-warning" label="Credentials stored" /></template>
        <template #actions="{ row }">
          <UiTooltip v-if="row.target_supplied?.length" :text="'Deploy through a target: it supplies ' + needs(row).join(', ')" position="left">
            <UiButton size="sm" variant="soft" icon="mdi-play" disabled :data-test="'config-deploy-' + row.id">Deploy</UiButton>
          </UiTooltip>
          <UiButton v-else size="sm" variant="soft" icon="mdi-play" :data-test="'config-deploy-' + row.id" @click="startDeploy(row)">Deploy</UiButton>
        </template>
      </UiDataTable>
    </UiCard>

    <UiDrawer v-model="drawer" :title="readOnly ? 'Configuration' : selected ? 'Edit configuration' : 'New configuration'" size="lg">
      <UiAlert v-if="error" kind="error" class="mb-3">{{ error }}</UiAlert>
      <UiSkeleton v-if="loadingDetail" kind="text" :lines="6" />
      <div v-else-if="readOnly && provider && detail" class="flex flex-col gap-4" data-test="config-view">
        <p class="text-sm text-base-content/70">{{ detail.name }} · {{ provider.display_name }}</p>
        <ProviderConfigForm mode="view" :provider="provider" :values="form.values" :credentials-set="secretKeys" />
      </div>
      <UiForm v-else :form="form">
        <div class="flex flex-col gap-4">
          <UiSelect id="provider_type" :key="providerKey" label="Provider" :options="providerOptions" :model-value="form.values.provider_type" :error="form.errors.value.provider_type" :hint="providerHint" :clearable="false" :disabled="!!selected" required placeholder="Choose a provider" data-test="config-provider" @update:model-value="chooseProvider" />
          <UiInput v-bind="form.field('name')" label="Name" required data-test="config-name" />
          <UiInput v-bind="form.field('description')" label="Description" />
          <template v-if="provider">
            <UiAlert v-if="legacyKeys.length" kind="warning" data-test="legacy-keys">Settings not recognised by this provider: {{ legacyKeys.join(', ') }} — they are removed when you save.</UiAlert>
            <ProviderConfigForm
              :provider="provider"
              :values="form.values"
              :errors="form.errors.value"
              :editing="!!selected"
              :credentials-set="credentialsSet"
              :cleared="cleared"
              :target-supplied="detail?.target_supplied ?? []"
              :options-open="optionsOpen"
              @update="(id, v) => (form.values[id] = v)"
              @blur="form.blur"
              @clear="onClear"
              @options-toggle="(o) => (optionsOpen = o)"
            >
              <template #field-host_ids="{ id, value, error: fieldError, required, hint, update, blur, field }">
                <HostPicker :id="id" :label="field.label" :model-value="value" :error="fieldError" :required="required" :hint="hint" :max="field.max_items" @update:model-value="update" @blur="blur" />
              </template>
            </ProviderConfigForm>
            <UiAlert v-if="leftToTargets.length" kind="warning" data-test="target-supplied-warning">
              This configuration cannot be deployed on its own: every target using it must supply {{ labels(provider, leftToTargets).join(', ') }}.
            </UiAlert>
            <div v-if="validated || validateError" class="flex flex-col gap-2" data-test="validate-result">
              <UiAlert v-if="validateError" kind="error">{{ validateError }}</UiAlert>
              <UiAlert v-else-if="validated" kind="success">{{ validatedText }}</UiAlert>
              <UiAlert v-if="unknownHosts.length" kind="warning">Unknown host ids: {{ unknownHosts.join(', ') }}</UiAlert>
              <UiDataTable v-if="validated?.details?.matched_hosts" :items="matched" :columns="matchedColumns" row-key="host_id" caption="Matched hosts" empty-title="No hosts match" data-test="matched-hosts">
                <template #cell-hostname="{ row }"><span class="flex flex-col"><span class="font-medium">{{ row.hostname }}</span><span v-if="row.os_name" class="text-xs text-base-content/70">{{ row.os_name }}</span></span></template>
                <template #cell-agent_online="{ row }">
                  <span class="flex flex-wrap gap-1">
                    <UiBadge :color="row.agent_online ? 'success' : 'neutral'" size="xs">{{ row.agent_online ? 'online' : 'offline' }}</UiBadge>
                    <UiBadge v-if="row.capability" :color="CAPABILITY[row.capability]?.color ?? 'neutral'" size="xs">{{ CAPABILITY[row.capability]?.text ?? row.capability }}</UiBadge>
                  </span>
                </template>
              </UiDataTable>
            </div>
          </template>
          <p v-else class="text-sm text-base-content/70" data-test="choose-provider">Choose a provider to see its settings.</p>
        </div>
      </UiForm>
      <template #actions>
        <UiButton v-if="selected && !readOnly" variant="text" color="error" @click="remove">Delete</UiButton>
        <UiButton v-if="provider && !readOnly" variant="soft" :loading="validating" data-test="config-validate" @click="check">{{ actionLabel }}</UiButton>
        <UiButton v-if="readOnly" variant="text" @click="drawer = false">Close</UiButton>
        <UiButton v-else :loading="form.submitting.value" data-test="config-save" @click="save">Save</UiButton>
      </template>
    </UiDrawer>

    <UiDialog v-model="deployOpen" :title="'Deploy to ' + (deployFor?.name ?? '')" size="sm">
      <UiForm :form="deployForm">
        <UiInput v-bind="deployForm.field('certificate_id')" label="Certificate ID" hint="The lcm certificate to deploy." required autocomplete="off" data-test="deploy-certificate" />
      </UiForm>
      <template #actions>
        <UiButton variant="text" @click="deployOpen = false">Cancel</UiButton>
        <UiButton :loading="deployForm.submitting.value" data-test="deploy-submit" @click="deployForm.submit()">Deploy</UiButton>
      </template>
    </UiDialog>
  </UiPage>
</template>
