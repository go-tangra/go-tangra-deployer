<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { UiPage, UiAlert, UiCard, UiButton, UiDataTable, UiStatusChip, UiBadge, UiIcon, UiDrawer, UiForm, UiInput, UiSelect, UiTextarea, useConfirm, useListQuery, useToast, type Column, type SelectOption } from '@go-tangra/ui'
import { useZodForm } from '@go-tangra/ui/forms'
import { CONFIG_LIST, useConfigurations } from '@/stores/configurations'
import { useProviders } from '@/stores/providers'
import { describe } from '@/api/client'
import { configurationSchema } from '@/schemas'
import type { Configuration, ConfigurationInput } from '@/api/types'

const store = useConfigurations()
const providers = useProviders()
const confirm = useConfirm()
const toast = useToast()
const drawer = ref(false)
const selected = ref<Configuration | null>(null)
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
const provider = computed(() => providers.get(String(form.values.provider_type ?? '')))

// Credentials are write-only: sealed at rest, never returned, never kept in the form after submit.
const build = (v: ReturnType<typeof configurationSchema.parse>): ConfigurationInput => ({ name: v.name, description: v.description, provider_type: v.provider_type, config: v.config, ...(Object.keys(v.credentials).length ? { credentials: v.credentials } : {}) })
const form = useZodForm(configurationSchema, {
  onSubmit: async (v) => {
    const input = build(v)
    if (selected.value) await store.update(selected.value.id, input)
    else await store.create(input)
  },
  onSuccess: () => {
    drawer.value = false
    void load()
  },
})
function open(c: Configuration | null): void {
  selected.value = c
  error.value = ''
  form.reset({ name: c?.name ?? '', description: c?.description ?? '', provider_type: c?.provider_type ?? '', config: JSON.stringify(c?.config ?? {}, null, 2), credentials: '' })
  drawer.value = true
}
async function validate(): Promise<void> {
  const v = form.validate()
  if (!v) return
  error.value = ''
  try {
    await store.validate(v.provider_type, v.credentials, v.config)
    toast.success('Credentials are valid.')
  } catch (e) {
    error.value = describe(e)
  }
}
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
        <template #cell-provider_type="{ row }"><UiBadge>{{ row.provider_type }}</UiBadge></template>
        <template #cell-status="{ row }"><UiStatusChip :status="row.status" :colors="{ inactive: 'neutral' }" /></template>
        <template #cell-has_credentials="{ row }"><UiIcon v-if="row.has_credentials" name="mdi-key" size="sm" class="text-warning" label="Credentials stored" /></template>
      </UiDataTable>
    </UiCard>
    <UiDrawer v-model="drawer" :title="selected ? 'Edit configuration' : 'New configuration'" size="lg">
      <UiAlert v-if="error" kind="error" class="mb-3">{{ error }}</UiAlert>
      <UiForm :form="form">
        <div class="flex flex-col gap-3">
          <UiInput v-bind="form.field('name')" label="Name" required data-test="config-name" />
          <UiInput v-bind="form.field('description')" label="Description" />
          <UiSelect v-bind="form.field('provider_type')" label="Provider" :options="providerOptions" :clearable="false" :disabled="!!selected" required data-test="config-provider" />
          <UiTextarea v-bind="form.field('config')" label="Config (JSON)" :rows="4" :hint="provider?.required_config?.length ? 'Keys: ' + provider.required_config.join(', ') : undefined" />
          <UiTextarea v-bind="form.field('credentials')" label="Credentials (JSON, write-only)" :rows="4" :hint="(provider?.required_credentials?.length ? 'Keys: ' + provider.required_credentials.join(', ') + '. ' : '') + 'Sealed at rest; never returned. Leave blank to keep existing.'" data-test="config-credentials" />
        </div>
      </UiForm>
      <template #actions>
        <UiButton v-if="selected" variant="text" color="error" @click="remove">Delete</UiButton>
        <UiButton variant="soft" @click="validate">Validate</UiButton>
        <UiButton :loading="form.submitting.value" data-test="config-save" @click="form.submit()">Save</UiButton>
      </template>
    </UiDrawer>
  </UiPage>
</template>
