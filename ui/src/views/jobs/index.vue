<script setup lang="ts">
// Deployment jobs: a filter bar (status, type), a server-paged table (whole-list
// header sorting on the server's sort fields; page / size / sort in the URL:
// ?jobs.page=…), live status patches, and a detail drawer whose child jobs and
// history are server-paged too.
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { UiPage, UiAlert, UiCard, UiForm, UiSelect, UiButton, UiDataTable, UiStatusChip, UiBadge, UiLiveIndicator, UiDrawer, UiKeyValueTable, useListQuery, type Column, type SelectOption } from '@go-tangra/ui'
import { useZodForm } from '@go-tangra/ui/forms'
import { DETAIL_LIST, JOB_LIST, useJobs, type JobFilter } from '@/stores/jobs'
import { useLive } from '@/stores/live'
import { describe } from '@/api/client'
import { jobFilterSchema, JOB_STATUSES, JOB_TYPES } from '@/schemas'
import type { ActionResult, DeliveryCounts, HistoryEntry, HostResult, Job } from '@/api/types'

const store = useJobs()
const live = useLive()
const drawer = ref(false)
const selected = ref<Job | null>(null)
const action = ref<ActionResult | null>(null)
const error = ref('')
const busy = ref(false)

// --- the jobs table ---
const lq = useListQuery('jobs', JOB_LIST)
const current = ref<JobFilter>({})
async function fetchJobs(): Promise<void> {
  const res = await store.list(current.value, lq.query.value)
  if (res?.page) lq.clampTo(res.page) // a page beyond the end answers the last page
}
watch(lq.query, () => void fetchJobs())
/** Filters changed: back to page 1 (which reloads), or reload in place. */
function applyFilter(f: JobFilter): void {
  current.value = { status: f.status, job_type: f.job_type }
  if (lq.page.value !== 1) lq.resetPage()
  else void fetchJobs()
}

let release: (() => void) | null = null
onMounted(() => {
  void fetchJobs()
  release = live.connect()
})
onUnmounted(() => release?.())
const statusOptions: SelectOption[] = JOB_STATUSES.map((s) => ({ title: s, value: s }))
const typeOptions: SelectOption[] = JOB_TYPES.map((s) => ({ title: s, value: s }))
const filter = useZodForm(jobFilterSchema, { onSubmit: (f) => applyFilter({ status: f.status, job_type: f.job_type }) })
const reload = () => void filter.submit()
/** The refresh button reloads the current page (filter, page and order kept). */
const refresh = () => void fetchJobs()

const statusColors = { partial: 'warning', processing: 'info', retrying: 'warning', cancelled: 'neutral', pending: 'neutral' } as const
const progressClass: Record<string, string> = { completed: 'progress-success', failed: 'progress-error', partial: 'progress-warning', processing: 'progress-info', retrying: 'progress-warning', cancelled: 'progress-neutral', pending: 'progress-neutral' }
const when = (s?: string) => (s ? new Date(s).toLocaleString() : '')
// Only the server's sort fields (JOB_LIST) are sortable; job_type shows row.type.
const columns: Column<Job>[] = [
  { key: 'certificate_id', label: 'Certificate' },
  { key: 'job_type', label: 'Type', width: 'sm', sortable: true },
  { key: 'triggered_by', label: 'Trigger', hideOnStack: true },
  { key: 'status', label: 'Status', width: 'sm', sortable: true },
  { key: 'progress', label: 'Progress', width: 'md', format: (j) => j.progress + '%' },
  { key: 'created_at', label: 'Created', format: (j) => when(j.created_at), sortable: true, defaultDir: 'desc', hideOnStack: true },
  { key: 'completed_at', label: 'Completed', format: (j) => when(j.completed_at), sortable: true, defaultDir: 'desc', hideOnStack: true },
]

// --- the detail drawer: server-paged child jobs and history ---
type HistoryRow = HistoryEntry & { id: string }
const children = ref<Job[]>([])
const childTotal = ref(0)
const history = ref<HistoryRow[]>([])
const historyTotal = ref(0)
const lqc = useListQuery('children', DETAIL_LIST)
const lqh = useListQuery('history', DETAIL_LIST)
async function fetchChildren(): Promise<void> {
  const j = selected.value
  if (!j || j.type !== 'parent') return
  try {
    const res = await lqc.track(store.children(j.id, lqc.query.value))
    if (!res) return
    children.value = res.items ?? []
    childTotal.value = res.total ?? 0
    lqc.clampTo(res.page)
  } catch (e) {
    error.value = describe(e)
  }
}
async function fetchHistory(): Promise<void> {
  const j = selected.value
  if (!j) return
  try {
    const res = await lqh.track(store.history(j.id, lqh.query.value))
    if (!res) return
    history.value = (res.items ?? []).map((h, i) => ({ ...h, id: h.id ?? String(i) }))
    historyTotal.value = res.total ?? 0
    lqh.clampTo(res.page)
  } catch (e) {
    error.value = describe(e)
  }
}
watch(lqc.query, () => drawer.value && void fetchChildren())
watch(lqh.query, () => drawer.value && void fetchHistory())
// --- per-host results of a by-reference delivery (inventory-agent) ---
interface Delivery { name?: string | undefined; counts: DeliveryCounts; hosts: (HostResult & Record<string, unknown>)[]; truncated: boolean; unknown: string[] }
const delivery = ref<Delivery | null>(null)
async function fetchResult(): Promise<void> {
  const j = selected.value
  if (!j || j.type === 'parent') return
  try {
    const d = (await store.result(j.id)).result?.details
    if (selected.value?.id !== j.id || !d || typeof d.counts !== 'object' || !Array.isArray(d.hosts)) return
    delivery.value = { name: typeof d.name === 'string' ? d.name : undefined, counts: d.counts as DeliveryCounts, hosts: (d.hosts as HostResult[]).map((h) => ({ ...h, id: h.host_id })), truncated: d.hosts_truncated === true, unknown: Array.isArray(d.unknown_host_ids) ? (d.unknown_host_ids as string[]) : [] }
  } catch {
    /* the result is optional detail; history still shows the outcome */
  }
}
const COUNT_LABELS: [keyof DeliveryCounts, string, 'success' | 'info' | 'warning' | 'error' | 'neutral'][] = [
  ['installed', 'Installed', 'success'],
  ['unchanged', 'Unchanged', 'success'],
  ['queued', 'Queued', 'info'],
  ['failed', 'Failed', 'error'],
  ['unsupported', 'Unsupported', 'warning'],
  ['superseded', 'Superseded', 'neutral'],
]
const hostStateColors = { installed: 'success', unchanged: 'success', pending: 'neutral', delivered: 'info', fetched: 'info', failed: 'error', hook_failed: 'error', unsupported: 'warning', superseded: 'neutral', cancelled: 'neutral', expired: 'warning' } as const
const hostColumns: Column<HostResult & Record<string, unknown>>[] = [
  { key: 'hostname', label: 'Host', format: (h) => h.hostname || h.host_id },
  { key: 'state', label: 'State', width: 'sm' },
  { key: 'agent_online', label: 'Agent', width: 'sm', hideOnStack: true },
  { key: 'reason', label: 'Reason' },
]

async function load(): Promise<void> {
  error.value = ''
  await Promise.all([fetchChildren(), fetchHistory(), fetchResult()])
}
function open(j: Job): void {
  selected.value = j
  action.value = null
  children.value = []
  childTotal.value = 0
  history.value = []
  historyTotal.value = 0
  delivery.value = null
  drawer.value = true
  // Another job starts at page 1 of its children and history. A page change
  // also fires the query watchers; track() drops the superseded response.
  lqc.resetPage()
  lqh.resetPage()
  void load()
}
async function run(fn: () => Promise<unknown>): Promise<void> {
  busy.value = true
  error.value = ''
  try {
    await fn()
    await load()
    void store.reload()
  } catch (e) {
    error.value = describe(e)
  } finally {
    busy.value = false
  }
}
const job = computed(() => (selected.value ? (store.items.find((x) => x.id === selected.value!.id) ?? selected.value) : null))
const cancel = () => job.value && run(() => store.cancel(job.value!.id))
const retry = () => job.value && run(() => store.retry(job.value!.id))
const verify = () => job.value && run(async () => (action.value = await store.verify(job.value!.id)))
const rollback = () => job.value && run(async () => (action.value = await store.rollback(job.value!.id)))
const meta = computed(() => (job.value ? [{ label: 'Certificate', value: job.value.certificate_id, copyable: true }, { label: 'Serial', value: job.value.certificate_serial }, { label: 'Retries', value: `${job.value.retry_count} / ${job.value.max_retries}` }, { label: 'Message', value: job.value.status_message }] : []))
const childColumns: Column<Job>[] = [{ key: 'target_configuration_id', label: 'Configuration' }, { key: 'status', label: 'Status', width: 'sm' }, { key: 'progress', label: 'Progress', align: 'end', format: (c) => c.progress + '%' }, { key: 'created_at', label: 'Created', sortable: true, defaultDir: 'desc', hideOnStack: true, format: (c) => when(c.created_at) }]
const historyColumns: Column<HistoryRow>[] = [{ key: 'action', label: 'Action' }, { key: 'result', label: 'Result', width: 'sm' }, { key: 'duration_ms', label: 'Duration', align: 'end', format: (h) => h.duration_ms + ' ms' }, { key: 'message', label: 'Message' }, { key: 'created_at', label: 'When', sortable: true, defaultDir: 'desc', hideOnStack: true, format: (h) => when(h.created_at) }]
</script>

<template>
  <UiPage title="Deployment jobs">
    <template #badges><UiLiveIndicator :connected="live.connected" /></template>
    <template #actions><UiButton variant="text" icon="mdi-refresh" icon-only label="Refresh" data-test="jobs-refresh" @click="refresh" /></template>
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
      <UiDataTable :items="store.items" :columns="columns" :loading="store.loading" :total="store.total" :page="lq.page.value" :page-size="lq.pageSize.value" :sort="lq.sort.value" caption="Jobs" empty-title="No jobs yet" clickable :row-attrs="(j) => ({ 'data-test': 'job-row-' + j.id })" data-test="jobs-table" @row-click="open" @update:page="lq.setPage" @update:page-size="lq.setPageSize" @update:sort="lq.setSort">
        <template #cell-job_type="{ row }"><UiBadge>{{ row.type }}</UiBadge></template>
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
        <section v-if="delivery" class="mb-3" data-test="job-hosts">
          <h3 class="mb-1 text-sm font-medium">Hosts<template v-if="delivery.name"> · {{ delivery.name }}</template></h3>
          <div class="mb-2 flex flex-wrap gap-1" data-test="job-counts">
            <template v-for="[k, label, color] in COUNT_LABELS" :key="k"><UiBadge v-if="delivery.counts[k]" :color="color">{{ label }} {{ delivery.counts[k] }}</UiBadge></template>
            <UiBadge>Total {{ delivery.counts.total ?? delivery.hosts.length }}</UiBadge>
          </div>
          <UiAlert v-if="delivery.unknown.length" kind="warning" class="mb-2">Unknown host ids: {{ delivery.unknown.join(', ') }}</UiAlert>
          <UiDataTable :items="delivery.hosts" :columns="hostColumns" row-key="host_id" caption="Host results" empty-title="No hosts">
            <template #cell-state="{ row }"><UiStatusChip :status="row.state" :colors="hostStateColors" /></template>
            <template #cell-agent_online="{ row }"><UiBadge :color="row.agent_online ? 'success' : 'neutral'" size="xs">{{ row.agent_online ? 'online' : 'offline' }}</UiBadge></template>
          </UiDataTable>
          <p v-if="delivery.truncated" class="mt-1 text-xs text-base-content/70">Only the first {{ delivery.hosts.length }} hosts are listed.</p>
        </section>
        <template v-if="childTotal">
          <h3 class="mb-1 text-sm font-medium">Child deployments</h3>
          <UiDataTable :items="children" :columns="childColumns" :total="childTotal" :page="lqc.page.value" :page-size="lqc.pageSize.value" :sort="lqc.sort.value" caption="Child deployments" class="mb-3" data-test="job-children" @update:page="lqc.setPage" @update:page-size="lqc.setPageSize" @update:sort="lqc.setSort">
            <template #cell-status="{ row }"><UiStatusChip :status="row.status" :colors="statusColors" /></template>
          </UiDataTable>
        </template>
        <template v-if="historyTotal">
          <h3 class="mb-1 text-sm font-medium">History</h3>
          <UiDataTable :items="history" :columns="historyColumns" :total="historyTotal" :page="lqh.page.value" :page-size="lqh.pageSize.value" :sort="lqh.sort.value" caption="History" data-test="job-history" @update:page="lqh.setPage" @update:page-size="lqh.setPageSize" @update:sort="lqh.setSort">
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
