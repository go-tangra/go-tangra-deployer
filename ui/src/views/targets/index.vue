<script setup lang="ts">
// Deployment targets: a server-paged list and the target drawer with
// certificate filters, attached configurations and, per attached
// configuration, its override form generated from the provider descriptors
// (feature 033, contracts/deployer-config-ui.md §4a, §6.11).
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { UiPage, UiAlert, UiCard, UiButton, UiDataTable, UiStatusChip, UiDrawer, UiDialog, UiForm, UiInput, UiSwitch, UiSelect, UiCheckbox, UiSection, UiBadge, UiIcon, UiTooltip, useConfirm, useListQuery, useToast, type Column, type SelectOption } from '@go-tangra/ui'
import { z } from 'zod'
import { useZodForm } from '@go-tangra/ui/forms'
import { TARGET_LIST, useTargets } from '@/stores/targets'
import { useConfigurations } from '@/stores/configurations'
import { useProviders } from '@/stores/providers'
import { ApiError, describe } from '@/api/client'
import { describeFieldErrors, filterToApi, filterToForm, overridePrefix, targetFormSchema } from '@/schemas'
import { configFields, labels, overridableFields, overrideValues, PATH_CONFIG } from '@/schemas/providerFields'
import ProviderConfigForm from '@/components/ProviderConfigForm.vue'
import HostPicker from '@/components/HostPicker.vue'
import CertificatePicker from '@/components/CertificatePicker.vue'
import type { Configuration, Target } from '@/api/types'

const store = useTargets()
const configs = useConfigurations()
const providers = useProviders()
const confirm = useConfirm()
const toast = useToast()
const drawer = ref(false)
const selected = ref<Target | null>(null)
const detail = ref<Target | null>(null)
const error = ref('')
// Server paging and sorting (page / size / sort in the URL: ?targets.page=…).
const lq = useListQuery('targets', TARGET_LIST)
async function load(): Promise<void> {
  const res = await store.list({}, lq.query.value)
  if (res?.page) lq.clampTo(res.page) // a page beyond the end answers the last page
}
watch(lq.query, () => void load())
onMounted(() => {
  void load()
  void configs.loadOptions() // every configuration, for the attachment picker
  void providers.list()
})

const configById = (id: string) => configs.options.find((c) => c.id === id)
const providerOf = (c: Configuration) => providers.get(c.provider_type)
const form = useZodForm(targetFormSchema({ configuration: configById, provider: (t) => providers.get(t) }), {
  onSubmit: async (v) => {
    const input = { name: v.name, description: v.description, auto_deploy: v.auto_deploy, certificate_filters: v.certificate_filters.map(filterToApi), notify_enabled: v.notify_enabled, notify_on: v.notify_on }
    const id = selected.value ? (await store.update(selected.value.id, input), selected.value.id) : (await store.create(input)).id
    // One attach call: new configurations and those whose override changed.
    const before = new Set(selected.value?.configuration_ids ?? [])
    const stored = detail.value?.config_overrides ?? {}
    const changed = v.configuration_ids.filter((c) => !before.has(c) || JSON.stringify(v.overrides[c] ?? {}) !== JSON.stringify(stored[c] ?? {}))
    if (changed.length) {
      try {
        await store.attach(id, changed, Object.fromEntries(changed.map((c) => [c, v.overrides[c] ?? {}])))
      } catch (e) {
        explain(e)
        throw e
      }
    }
    const toDetach = [...before].filter((c) => !v.configuration_ids.includes(c))
    if (toDetach.length) await store.detach(id, toDetach)
  },
  onSuccess: () => {
    drawer.value = false
    void load()
  },
})

/** Attach refusals concern one configuration: map them onto its override inputs. */
function explain(e: unknown): void {
  if (!(e instanceof ApiError) || e.reason !== 'validation_failed') return
  const d = e.detail as { fields?: Record<string, unknown>; configuration_id?: string } | undefined
  if (!d?.fields || !d.configuration_id) return
  const c = configById(d.configuration_id)
  d.fields = describeFieldErrors(d.fields, c && providerOf(c), undefined, overridePrefix(d.configuration_id))
  openCfgs.value = new Set([...openCfgs.value, d.configuration_id])
}

const filters = computed(() => (form.values.certificate_filters ?? []) as Record<string, string>[])
const attached = computed(() => (form.values.configuration_ids ?? []) as string[])
const attachedConfigs = computed(() => attached.value.map(configById).filter((c): c is Configuration => !!c))
/** Expanded override sections (configurations needing input start open). */
const openCfgs = ref(new Set<string>())

async function open(t: Target | null): Promise<void> {
  selected.value = t
  detail.value = null
  error.value = ''
  openCfgs.value = new Set()
  reset(t)
  drawer.value = true
  if (!t) return
  try {
    detail.value = await store.get(t.id) // overrides and missing_required
  } catch (e) {
    error.value = describe(e)
    return
  }
  await Promise.all([providers.list(), configs.options.length ? Promise.resolve() : configs.loadOptions()])
  reset(detail.value)
  openCfgs.value = new Set((detail.value.configuration_ids ?? []).filter((id) => needsInput(id) || (detail.value?.missing_required?.[id]?.length ?? 0) > 0))
}
function reset(t: Target | null): void {
  const overrides: Record<string, unknown> = {}
  for (const id of t?.configuration_ids ?? []) Object.assign(overrides, overrideInputs(id, t?.config_overrides?.[id]))
  form.reset({ name: t?.name ?? '', description: t?.description ?? '', auto_deploy: t?.auto_deploy ?? false, notify_enabled: t?.notify_enabled ?? false, notify_on: t?.notify_on ?? 'always', certificate_filters: (t?.certificate_filters ?? []).map(filterToForm), configuration_ids: [...(t?.configuration_ids ?? [])], ...overrides })
}
function overrideInputs(id: string, override?: Record<string, unknown>): Record<string, unknown> {
  const c = configById(id)
  const p = c && providerOf(c)
  return p ? overrideValues(p, override, overridePrefix(id)) : {}
}
const needsInput = (id: string) => (configById(id)?.target_supplied?.length ?? 0) > 0

function addFilter(): void {
  form.values.certificate_filters = [...filters.value, { issuer: '', common_name: '', san: '', organization: '', org_unit: '', country: '' }]
}
function removeFilter(i: number): void {
  form.values.certificate_filters = filters.value.filter((_, j) => j !== i)
}
function toggleConfig(id: string, on: unknown): void {
  if (on) {
    form.values.configuration_ids = [...new Set([...attached.value, id])]
    Object.assign(form.values, overrideInputs(id, detail.value?.config_overrides?.[id]))
    if (needsInput(id)) openCfgs.value = new Set([...openCfgs.value, id])
  } else {
    form.values.configuration_ids = attached.value.filter((c) => c !== id)
    for (const k of Object.keys(form.values)) if (k.startsWith(overridePrefix(id))) delete form.values[k]
    form.errors.value = Object.fromEntries(Object.entries(form.errors.value).filter(([k]) => !k.startsWith(overridePrefix(id))))
  }
}
function onToggle(id: string, e: Event): void {
  if (!(e.target instanceof HTMLDetailsElement)) return
  const next = new Set(openCfgs.value)
  if (e.target.open) next.add(id)
  else next.delete(id)
  openCfgs.value = next
}
/** Refusals of required fields a target cannot supply (no input to show them on). */
const fixedErrors = (id: string) => Object.entries(form.errors.value).filter(([k]) => k.startsWith(overridePrefix(id) + PATH_CONFIG)).map(([k, v]) => {
  const c = configById(id)
  const key = k.slice((overridePrefix(id) + PATH_CONFIG).length)
  return `${(c && providerOf(c) && configFields(providerOf(c)!).find((f) => f.key === key)?.label) ?? key}: ${v}`
})
const mustSupply = (c: Configuration) => labels(providerOf(c), c.target_supplied ?? [])

async function save(): Promise<void> {
  // Refused override inputs inside collapsed sections must be focusable.
  if (!form.valid.value) {
    openCfgs.value = new Set([...openCfgs.value, ...attached.value])
    await nextTick()
  }
  await form.submit()
}
async function remove(t: Target | null = selected.value): Promise<void> {
  if (!t || !(await confirm.ask({ title: `Delete ${t.name}?`, text: 'Its configurations stay; they are only detached from this target. Past jobs are kept.', danger: true, confirmLabel: 'Delete' }))) return
  try {
    await store.remove(t.id)
    if (selected.value?.id === t.id) drawer.value = false
    void load()
  } catch (e) {
    if (drawer.value) error.value = describe(e)
    else toast.error(describe(e))
  }
}

// --- deploy to a target -----------------------------------------------------
// Like a configuration's direct deployment: pick lcm's certificate by host
// name; every configuration attached to the target deploys it.
const CERTIFICATE_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const deployFor = ref<Target | null>(null)
const deployOpen = ref(false)
const deployForm = useZodForm(z.object({ certificate_id: z.string().regex(CERTIFICATE_ID, 'Type a host name and pick a certificate.') }), {
  onSubmit: async (v) => store.deploy(v.certificate_id, deployFor.value!.id),
  onSuccess: (jobId) => {
    deployOpen.value = false
    toast.success(`Deployment started (job ${jobId}).`)
  },
})
function startDeploy(t: Target): void {
  deployFor.value = t
  deployForm.reset({ certificate_id: '' })
  deployOpen.value = true
}
const deployable = (t: Target) => (t.configuration_ids?.length ?? 0) > 0
// Only the server's sort fields (TARGET_LIST) are sortable.
const columns: Column<Target>[] = [
  { key: 'name', label: 'Name', sortable: true },
  { key: 'auto_deploy', label: 'Auto-deploy', width: 'sm', format: (t) => (t.auto_deploy ? 'on' : 'off') },
  { key: 'filters', label: 'Filters', align: 'end', format: (t) => String(t.certificate_filters?.length || 0) },
  { key: 'configurations', label: 'Configurations', align: 'end', format: (t) => String(t.configuration_ids?.length || 0) },
  { key: 'created_at', label: 'Created', sortable: true, defaultDir: 'desc', hideOnStack: true, format: (t) => (t.created_at ? new Date(t.created_at).toLocaleString() : '') },
]
// Status e-mails: who receives them and when.
const notifyOptions: SelectOption[] = [
  { title: 'Every finished deployment', value: 'always' },
  { title: 'Only failures', value: 'failure' },
]
const NOTIFY_HINT = 'Tenant owners, admins and deployer administrators receive an e-mail when a deployment to this target finishes.'
const filterErr = (i: number, k: string) => form.errors.value[`certificate_filters.${i}.${k}`]
</script>

<template>
  <UiPage title="Deployment targets">
    <template #actions><UiButton icon="mdi-plus" data-test="target-new" @click="open(null)">New target</UiButton></template>
    <UiAlert v-if="store.error" kind="error" class="mb-3">{{ store.error }}</UiAlert>
    <UiCard :padded="false">
      <UiDataTable :items="store.items" :columns="columns" :loading="store.loading" :total="store.total" :page="lq.page.value" :page-size="lq.pageSize.value" :sort="lq.sort.value" caption="Targets" empty-title="No targets yet" clickable :row-attrs="(t) => ({ 'data-test': 'target-row-' + t.id })" data-test="targets-table" @row-click="open" @update:page="lq.setPage" @update:page-size="lq.setPageSize" @update:sort="lq.setSort">
        <template #cell-auto_deploy="{ row }"><UiStatusChip :status="row.auto_deploy ? 'on' : 'off'" :colors="{ on: 'success', off: 'neutral' }" /></template>
        <template #actions="{ row }">
          <div class="flex gap-1">
            <UiTooltip v-if="!deployable(row)" text="Attach a configuration to deploy to this target" position="left">
              <UiButton size="sm" variant="soft" icon="mdi-play" disabled :data-test="'target-deploy-' + row.id">Deploy</UiButton>
            </UiTooltip>
            <UiButton v-else size="sm" variant="soft" icon="mdi-play" :data-test="'target-deploy-' + row.id" @click.stop="startDeploy(row)">Deploy</UiButton>
            <UiButton size="sm" variant="text" color="error" icon="mdi-delete-outline" :aria-label="'Delete ' + row.name" :data-test="'target-delete-' + row.id" @click.stop="remove(row)" />
          </div>
        </template>
      </UiDataTable>
    </UiCard>
    <UiDrawer v-model="drawer" :title="selected ? 'Edit target' : 'New target'" size="lg">
      <UiAlert v-if="error" kind="error" class="mb-3">{{ error }}</UiAlert>
      <UiForm :form="form">
        <div class="flex flex-col gap-4">
          <UiInput v-bind="form.field('name')" label="Name" required data-test="target-name" />
          <UiInput v-bind="form.field('description')" label="Description" />
          <UiSwitch v-bind="form.field('auto_deploy')" label="Auto-deploy on issue/renewal" data-test="target-auto" />
          <UiSwitch v-bind="form.field('notify_enabled')" label="Send notification" :hint="NOTIFY_HINT" data-test="target-notify" />
          <UiSelect v-if="form.values.notify_enabled" v-bind="form.field('notify_on')" label="Notify on" :options="notifyOptions" :clearable="false" data-test="target-notify-on" />
          <UiSection title="Certificate filters" description="Empty filters match all certificates. Rules within a filter are AND-matched.">
            <div v-for="(f, i) in filters" :key="i" class="mb-2 rounded-box border border-base-300 p-3">
              <div class="grid grid-cols-1 gap-2 md:grid-cols-2">
                <UiInput :id="'f-issuer-' + i" v-model="f.issuer" label="Issuer (exact)" size="sm" :error="filterErr(i, 'issuer')" />
                <UiInput :id="'f-cn-' + i" v-model="f.common_name" label="Common name (regex)" size="sm" :error="filterErr(i, 'common_name')" />
                <UiInput :id="'f-san-' + i" v-model="f.san" label="SAN (regex)" size="sm" :error="filterErr(i, 'san')" />
                <UiInput :id="'f-org-' + i" v-model="f.organization" label="Organization (exact)" size="sm" :error="filterErr(i, 'organization')" />
              </div>
              <div class="mt-2 flex justify-end"><UiButton size="xs" variant="text" color="error" icon="mdi-delete-outline" @click="removeFilter(i)">Remove filter</UiButton></div>
            </div>
            <UiButton size="sm" variant="soft" icon="mdi-plus" @click="addFilter">Add filter</UiButton>
          </UiSection>
          <UiSection title="Attached configurations" description="Each configuration deploys with its own settings; a target may supply or override the settings its provider allows.">
            <p v-if="!configs.options.length" class="text-sm text-base-content/70">No configurations yet.</p>
            <UiCheckbox v-for="c in configs.options" :id="'cfg-' + c.id" :key="c.id" :model-value="attached.includes(c.id)" :label="c.name + ' · ' + (providers.get(c.provider_type)?.display_name ?? c.provider_type)" data-test="target-configs" @update:model-value="toggleConfig(c.id, $event)" />
          </UiSection>
          <UiSection v-if="attachedConfigs.length" title="Settings per configuration" description="Leave a field empty to inherit the configuration's value.">
            <details v-for="c in attachedConfigs" :key="c.id" class="group mb-2 rounded-box border border-base-300" :open="openCfgs.has(c.id) || undefined" :data-test="'override-' + c.id" :data-configuration-id="c.id" @toggle="onToggle(c.id, $event)">
              <summary class="flex cursor-pointer list-none items-center justify-between gap-2 rounded-box px-4 py-3 hover:bg-base-200 [&::-webkit-details-marker]:hidden">
                <span class="flex flex-wrap items-center gap-2">
                  <span class="font-medium">{{ c.name }}</span>
                  <UiBadge size="xs">{{ providerOf(c)?.display_name ?? c.provider_type }}</UiBadge>
                  <UiBadge v-if="c.target_supplied?.length" color="warning" size="xs">Must supply: {{ mustSupply(c).join(', ') }}</UiBadge>
                </span>
                <UiIcon name="mdi-chevron-down" size="sm" class="shrink-0 transition-transform group-open:rotate-180" />
              </summary>
              <div class="flex flex-col gap-3 px-4 pt-1 pb-4">
                <UiAlert v-if="detail?.missing_required?.[c.id]?.length" kind="warning" data-test="missing-required">Missing required values: {{ detail.missing_required[c.id]!.join(', ') }} — deployments to this target fail until they are supplied.</UiAlert>
                <UiAlert v-for="m in fixedErrors(c.id)" :key="m" kind="error">{{ m }}</UiAlert>
                <p v-if="!providerOf(c)" class="text-sm text-base-content/70">Provider {{ c.provider_type }} is not available.</p>
                <p v-else-if="!overridableFields(providerOf(c)!).length" class="text-sm text-base-content/70">This provider has no settings a target can override.</p>
                <ProviderConfigForm
                  v-else
                  mode="override"
                  :provider="providerOf(c)!"
                  :values="form.values"
                  :errors="form.errors.value"
                  :id-prefix="overridePrefix(c.id)"
                  :inherited="c.config ?? {}"
                  :target-supplied="c.target_supplied ?? []"
                  @update="(id, v) => (form.values[id] = v)"
                  @blur="form.blur"
                >
                  <template #field-host_ids="{ id, value, error: fieldError, required, hint, update, blur, field }">
                    <HostPicker :id="id" :label="field.label" :model-value="value" :error="fieldError" :required="required" :hint="hint" :max="field.max_items" @update:model-value="update" @blur="blur" />
                  </template>
                </ProviderConfigForm>
              </div>
            </details>
          </UiSection>
        </div>
      </UiForm>
      <template #actions>
        <UiButton v-if="selected" variant="text" color="error" data-test="target-delete" @click="remove()">Delete</UiButton>
        <UiButton variant="text" @click="drawer = false">Cancel</UiButton>
        <UiButton :loading="form.submitting.value" data-test="target-save" @click="save">Save</UiButton>
      </template>
    </UiDrawer>

    <UiDialog v-model="deployOpen" :title="'Deploy to ' + (deployFor?.name ?? '')" size="sm">
      <p class="mb-3 text-sm text-base-content/70">Every configuration attached to this target ({{ deployFor?.configuration_ids?.length ?? 0 }}) deploys the certificate, with the target's settings.</p>
      <UiForm :form="deployForm">
        <CertificatePicker v-bind="deployForm.field('certificate_id')" :disabled="deployForm.submitting.value" />
      </UiForm>
      <template #actions>
        <UiButton variant="text" @click="deployOpen = false">Cancel</UiButton>
        <UiButton :loading="deployForm.submitting.value" data-test="target-deploy-submit" @click="deployForm.submit()">Deploy</UiButton>
      </template>
    </UiDialog>
  </UiPage>
</template>
