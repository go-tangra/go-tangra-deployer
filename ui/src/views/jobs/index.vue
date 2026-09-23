<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { UiPage, UiAlert, UiCard, UiForm, UiSelect, UiButton, UiDataTable, UiStatusChip, UiBadge, UiLiveIndicator, UiDrawer, UiKeyValueTable, type Column, type SelectOption } from '@freya/ui'
import { useZodForm } from '@freya/ui/forms'
import { useJobs } from '@/stores/jobs'
import { useLive } from '@/stores/live'
import { describe } from '@/api/client'
import { jobFilterSchema, JOB_STATUSES, JOB_TYPES } from '@/schemas'
import type { ActionResult, Job, JobResult } from '@/api/types'

const store = useJobs()
const live = useLive()
const drawer = ref(false)
const selected = ref<Job | null>(null)
const detail = ref<JobResult | null>(null)
const action = ref<ActionResult | null>(null)
const error = ref('')
const busy = ref(false)

let release: (() => void) | null = null
onMounted(() => {
  void store.list()
  release = live.connect()
})
onUnmounted(() => release?.())
const statusOptions: SelectOption[] = JOB_STATUSES.map((s) => ({ title: s, value: s }))
const typeOptions: SelectOption[] = JOB_TYPES.map((s) => ({ title: s, value: s }))
const filter = useZodForm(jobFilterSchema, { onSubmit: (f) => store.list({ status: f.status, job_type: f.job_type }) })
const reload = () => void filter.submit()

const statusColors = { partial: 'warning', processing: 'info', retrying: 'warning', cancelled: 'neutral', pending: 'neutral' } as const
const progressClass: Record<string, string> = { completed: 'progress-success', failed: 'progress-error', partial: 'progress-warning', processing: 'progress-info', retrying: 'progress-warning', cancelled: 'progress-neutral', pending: 'progress-neutral' }
const columns: Column<Job>[] = [
  { key: 'certificate_id', label: 'Certificate' },
  { key: 'type', label: 'Type', width: 'sm' },
  { key: 'triggered_by', label: 'Trigger', hideOnStack: true },
  { key: 'status', label: 'Status', width: 'sm' },
  { key: 'progress', label: 'Progress', width: 'md', format: (j) => j.progress + '%' },
  { key: 'created_at', label: 'Created', format: (j) => new Date(j.created_at).toLocaleString(), sortable: true, hideOnStack: true },
]

async function load(): Promise<void> {
  if (!selected.value) return
  error.value = ''
  action.value = null
  detail.value = null
  try {
    detail.value = await store.result(selected.value.id)
  } catch (e) {
    error.value = describe(e)
  }
}
watch(drawer, (open) => open && load())
function open(j: Job): void {
  selected.value = j
  drawer.value = true
}
async function run(fn: () => Promise<unknown>): Promise<void> {
  busy.value = true
  error.value = ''
  try {
    await fn()
    await load()
    reload()
  } catch (e) {
    error.value = describe(e)
  } finally {
    busy.value = false
  }
}
const job = computed(() => selected.value)
const cancel = () => job.value && run(() => store.cancel(job.value!.id))
const retry = () => job.value && run(() => store.retry(job.value!.id))
const verify = () => job.value && run(async () => (action.value = await store.verify(job.value!.id)))
const rollback = () => job.value && run(async () => (action.value = await store.rollback(job.value!.id)))
const meta = computed(() => (job.value ? [{ label: 'Certificate', value: job.value.certificate_id, copyable: true }, { label: 'Serial', value: job.value.certificate_serial }, { label: 'Retries', value: `${job.value.retry_count} / ${job.value.max_retries}` }, { label: 'Message', value: job.value.status_message }] : []))
const childColumns: Column<Job>[] = [{ key: 'target_configuration_id', label: 'Configuration' }, { key: 'status', label: 'Status', width: 'sm' }, { key: 'progress', label: 'Progress', align: 'end', format: (c) => c.progress + '%' }]
const historyRows = computed(() => (detail.value?.history ?? []).map((h, i) => ({ ...h, id: String(i) })))
const historyColumns: Column<(typeof historyRows.value)[number]>[] = [{ key: 'action', label: 'Action' }, { key: 'result', label: 'Result', width: 'sm' }, { key: 'duration_ms', label: 'Duration', align: 'end', format: (h) => h.duration_ms + ' ms' }, { key: 'message', label: 'Message' }]
</script>

<template>
  <UiPage title="Deployment jobs">
    <template #badges><UiLiveIndicator :connected="live.connected" /></template>
    <template #actions><UiButton variant="text" icon="mdi-refresh" icon-only label="Refresh" @click="reload" /></template>
    <template #filters>
      <UiForm :form="filter" class="w-full">
        <div class="grid grid-cols-2 gap-2 md:max-w-md">
          <UiSelect v-bind="filter.field('status')" label="Status" :options="statusOptions" size="sm" @update:model-value="reload" />
          <UiSelect v-bind="filter.field('job_type')" label="Type" :options="typeOptions" size="sm" @update:model-value="reload" />
        </div>
      </UiForm>
    </template>
    <UiAlert v-if="store.error" kind="error" class="mb-3">{{ store.error }}</UiAlert>
    <UiCard :padded="false">
      <UiDataTable :items="store.items" :columns="columns" :loading="store.loading" caption="Jobs" empty-title="No jobs yet" clickable :row-attrs="(j) => ({ 'data-test': 'job-row-' + j.id })" data-test="jobs-table" @row-click="open">
        <template #cell-type="{ row }"><UiBadge>{{ row.type }}</UiBadge></template>
        <template #cell-status="{ row }"><UiStatusChip :status="row.status" :colors="statusColors" /></template>
        <template #cell-progress="{ row }"><progress class="progress h-2 w-24" :class="progressClass[row.status]" :value="row.progress" max="100" :aria-label="'Progress ' + row.progress + '%'" /></template>
      </UiDataTable>
    </UiCard>
    <UiDrawer v-model="drawer" title="Deployment job" size="lg">
      <template v-if="job">
        <UiAlert v-if="error" kind="error" class="mb-3">{{ error }}</UiAlert>
        <UiAlert v-if="action" :kind="action.success ? 'success' : 'warning'" class="mb-3">{{ action.action }}: {{ action.message || (action.success ? 'ok' : 'failed') }}</UiAlert>
        <div class="mb-2 flex flex-wrap gap-1"><UiStatusChip :status="job.status" :colors="statusColors" /><UiBadge>{{ job.type }}</UiBadge><UiBadge>{{ job.triggered_by }}</UiBadge></div>
        <progress class="progress mb-3 h-1.5 w-full" :class="progressClass[job.status]" :value="job.progress" max="100" aria-label="Job progress" />
        <UiKeyValueTable :items="meta" class="mb-3" />
        <template v-if="detail?.children?.length">
          <h3 class="mb-1 text-sm font-medium">Child deployments</h3>
          <UiDataTable :items="detail.children" :columns="childColumns" caption="Child deployments" class="mb-3">
            <template #cell-status="{ row }"><UiStatusChip :status="row.status" :colors="statusColors" /></template>
          </UiDataTable>
        </template>
        <template v-if="historyRows.length">
          <h3 class="mb-1 text-sm font-medium">History</h3>
          <UiDataTable :items="historyRows" :columns="historyColumns" caption="History">
            <template #cell-result="{ row }"><UiStatusChip :status="row.result" :colors="{ success: 'success', failure: 'error', partial: 'warning' }" /></template>
          </UiDataTable>
        </template>
      </template>
      <template #actions>
        <UiButton v-if="job && ['pending', 'processing', 'retrying'].includes(job.status)" size="sm" variant="soft" color="error" :loading="busy" @click="cancel">Cancel</UiButton>
        <UiButton v-if="job && ['failed', 'partial'].includes(job.status)" size="sm" variant="soft" :loading="busy" @click="retry">Retry</UiButton>
        <UiButton v-if="job && job.type !== 'parent'" size="sm" variant="soft" :loading="busy" @click="verify">Verify</UiButton>
        <UiButton v-if="job && job.type !== 'parent'" size="sm" variant="soft" :loading="busy" @click="rollback">Rollback</UiButton>
      </template>
    </UiDrawer>
  </UiPage>
</template>
