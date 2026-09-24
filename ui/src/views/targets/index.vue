<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { UiPage, UiAlert, UiCard, UiButton, UiDataTable, UiStatusChip, UiDrawer, UiForm, UiInput, UiTextarea, UiSwitch, UiCheckbox, UiSection, useConfirm, type Column } from '@go-tangra/ui'
import { useZodForm } from '@go-tangra/ui/forms'
import { useTargets } from '@/stores/targets'
import { useConfigurations } from '@/stores/configurations'
import { describe } from '@/api/client'
import { targetSchema } from '@/schemas'
import type { CertificateFilter, Target } from '@/api/types'

const store = useTargets()
const configs = useConfigurations()
const confirm = useConfirm()
const drawer = ref(false)
const selected = ref<Target | null>(null)
const error = ref('')
onMounted(() => {
  void store.list()
  void configs.list()
})

const form = useZodForm(targetSchema, {
  onSubmit: async (v) => {
    const input = { name: v.name, description: v.description, auto_deploy: v.auto_deploy, certificate_filters: v.certificate_filters as CertificateFilter[] }
    const id = selected.value ? (await store.update(selected.value.id, input), selected.value.id) : (await store.create(input)).id
    // Reconcile configuration attachments.
    const before = new Set(selected.value?.configuration_ids ?? [])
    const now = new Set(v.configuration_ids)
    const toAttach = [...now].filter((c) => !before.has(c))
    const toDetach = [...before].filter((c) => !now.has(c))
    if (toAttach.length) await store.attach(id, toAttach, Object.keys(v.overrides).length ? v.overrides : undefined)
    if (toDetach.length) await store.detach(id, toDetach)
  },
  onSuccess: () => {
    drawer.value = false
    void store.list()
  },
})
const filters = computed(() => (form.values.certificate_filters ?? []) as Record<string, string>[])
const attached = computed(() => (form.values.configuration_ids ?? []) as string[])
function open(t: Target | null): void {
  selected.value = t
  error.value = ''
  form.reset({ name: t?.name ?? '', description: t?.description ?? '', auto_deploy: t?.auto_deploy ?? false, certificate_filters: (t?.certificate_filters ?? []).map((f) => ({ issuer: f.issuer ?? '', common_name: f.common_name ?? '', san: f.san ?? '', organization: f.organization ?? '' })), configuration_ids: [...(t?.configuration_ids ?? [])], overrides: '' })
  drawer.value = true
}
function addFilter(): void {
  form.values.certificate_filters = [...filters.value, { issuer: '', common_name: '', san: '', organization: '' }]
}
function removeFilter(i: number): void {
  form.values.certificate_filters = filters.value.filter((_, j) => j !== i)
}
function toggleConfig(id: string, on: unknown): void {
  form.values.configuration_ids = on ? [...new Set([...attached.value, id])] : attached.value.filter((c) => c !== id)
}
async function remove(): Promise<void> {
  if (!selected.value || !(await confirm.ask({ title: `Delete ${selected.value.name}?`, danger: true, confirmLabel: 'Delete' }))) return
  try {
    await store.remove(selected.value.id)
    drawer.value = false
    void store.list()
  } catch (e) {
    error.value = describe(e)
  }
}
const columns: Column<Target>[] = [
  { key: 'name', label: 'Name', sortable: true },
  { key: 'auto_deploy', label: 'Auto-deploy', width: 'sm', format: (t) => (t.auto_deploy ? 'on' : 'off') },
  { key: 'filters', label: 'Filters', align: 'end', format: (t) => String(t.certificate_filters?.length || 0) },
  { key: 'configurations', label: 'Configurations', align: 'end', format: (t) => String(t.configuration_ids?.length || 0) },
]
const filterErr = (i: number, k: string) => form.errors.value[`certificate_filters.${i}.${k}`]
</script>

<template>
  <UiPage title="Deployment targets">
    <template #actions><UiButton icon="mdi-plus" data-test="target-new" @click="open(null)">New target</UiButton></template>
    <UiAlert v-if="store.error" kind="error" class="mb-3">{{ store.error }}</UiAlert>
    <UiCard :padded="false">
      <UiDataTable :items="store.items" :columns="columns" :loading="store.loading" caption="Targets" empty-title="No targets yet" clickable :row-attrs="(t) => ({ 'data-test': 'target-row-' + t.id })" data-test="targets-table" @row-click="open">
        <template #cell-auto_deploy="{ row }"><UiStatusChip :status="row.auto_deploy ? 'on' : 'off'" :colors="{ on: 'success', off: 'neutral' }" /></template>
      </UiDataTable>
    </UiCard>
    <UiDrawer v-model="drawer" :title="selected ? 'Edit target' : 'New target'" size="lg">
      <UiAlert v-if="error" kind="error" class="mb-3">{{ error }}</UiAlert>
      <UiForm :form="form">
        <div class="flex flex-col gap-3">
          <UiInput v-bind="form.field('name')" label="Name" required data-test="target-name" />
          <UiInput v-bind="form.field('description')" label="Description" />
          <UiSwitch v-bind="form.field('auto_deploy')" label="Auto-deploy on issue/renewal" data-test="target-auto" />
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
          <UiSection title="Attached configurations">
            <p v-if="!configs.items.length" class="text-sm text-base-content/70">No configurations yet.</p>
            <UiCheckbox v-for="c in configs.items" :id="'cfg-' + c.id" :key="c.id" :model-value="attached.includes(c.id)" :label="c.name + ' (' + c.provider_type + ')'" data-test="target-configs" @update:model-value="toggleConfig(c.id, $event)" />
            <UiTextarea v-bind="form.field('overrides')" label="Per-config overrides (JSON: {configId: {...}})" :rows="3" hint="Config overlay only — never credentials. Applied to newly attached configs." class="mt-2" />
          </UiSection>
        </div>
      </UiForm>
      <template #actions>
        <UiButton v-if="selected" variant="text" color="error" @click="remove">Delete</UiButton>
        <UiButton variant="text" @click="drawer = false">Cancel</UiButton>
        <UiButton :loading="form.submitting.value" data-test="target-save" @click="form.submit()">Save</UiButton>
      </template>
    </UiDrawer>
  </UiPage>
</template>
