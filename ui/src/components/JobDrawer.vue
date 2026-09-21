<script setup lang="ts">
import { ref, watch } from 'vue'
import { useJobs } from '@/stores/jobs'
import { describe } from '@/api/client'
import type { ActionResult, Job, JobResult } from '@/api/types'

const props = defineProps<{ modelValue: boolean; job: Job | null }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; changed: [] }>()

const store = useJobs()
const detail = ref<JobResult | null>(null)
const error = ref('')
const action = ref<ActionResult | null>(null)
const busy = ref(false)

const statusColor: Record<string, string> = {
  completed: 'success',
  failed: 'error',
  partial: 'warning',
  processing: 'info',
  retrying: 'warning',
  cancelled: 'grey',
  pending: 'grey',
}

async function load(): Promise<void> {
  if (!props.job) return
  error.value = ''
  action.value = null
  detail.value = null
  try {
    detail.value = await store.result(props.job.id)
  } catch (e) {
    error.value = describe(e)
  }
}

watch(() => props.modelValue, (open) => open && load())

async function run(fn: () => Promise<unknown>): Promise<void> {
  busy.value = true
  error.value = ''
  try {
    await fn()
    await load()
    emit('changed')
  } catch (e) {
    error.value = describe(e)
  } finally {
    busy.value = false
  }
}

function cancel(): void {
  if (props.job) void run(() => store.cancel(props.job!.id))
}
function retry(): void {
  if (props.job) void run(() => store.retry(props.job!.id))
}
async function verify(): Promise<void> {
  if (!props.job) return
  await run(async () => (action.value = await store.verify(props.job!.id)))
}
async function rollback(): Promise<void> {
  if (!props.job) return
  await run(async () => (action.value = await store.rollback(props.job!.id)))
}
</script>

<template>
  <v-navigation-drawer :model-value="modelValue" location="right" temporary width="500" @update:model-value="emit('update:modelValue', $event)">
    <v-toolbar density="comfortable" title="Deployment job">
      <v-btn icon="mdi-close" variant="text" @click="emit('update:modelValue', false)" />
    </v-toolbar>
    <div v-if="job" class="pa-4">
      <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-3">{{ error }}</v-alert>
      <v-alert v-if="action" :type="action.success ? 'success' : 'warning'" variant="tonal" density="compact" class="mb-3">
        {{ action.action }}: {{ action.message || (action.success ? 'ok' : 'failed') }}
      </v-alert>

      <div class="d-flex align-center mb-2">
        <v-chip :color="statusColor[job.status]" size="small" variant="flat">{{ job.status }}</v-chip>
        <v-chip class="ms-2" size="small" variant="tonal">{{ job.type }}</v-chip>
        <v-chip class="ms-2" size="small" variant="tonal">{{ job.triggered_by }}</v-chip>
      </div>
      <v-progress-linear :model-value="job.progress" height="6" rounded class="mb-3" :color="statusColor[job.status]" />

      <v-table density="compact" class="mb-3">
        <tbody>
          <tr><td class="text-medium-emphasis">Certificate</td><td>{{ job.certificate_id }}</td></tr>
          <tr v-if="job.certificate_serial"><td class="text-medium-emphasis">Serial</td><td>{{ job.certificate_serial }}</td></tr>
          <tr><td class="text-medium-emphasis">Retries</td><td>{{ job.retry_count }} / {{ job.max_retries }}</td></tr>
          <tr v-if="job.status_message"><td class="text-medium-emphasis">Message</td><td>{{ job.status_message }}</td></tr>
        </tbody>
      </v-table>

      <template v-if="detail?.children?.length">
        <div class="text-subtitle-2 mb-1">Child deployments</div>
        <v-table density="compact" class="mb-3">
          <tbody>
            <tr v-for="c in detail.children" :key="c.id">
              <td>{{ c.target_configuration_id }}</td>
              <td><v-chip :color="statusColor[c.status]" size="x-small" variant="flat">{{ c.status }}</v-chip></td>
              <td>{{ c.progress }}%</td>
            </tr>
          </tbody>
        </v-table>
      </template>

      <template v-if="detail?.history?.length">
        <div class="text-subtitle-2 mb-1">History</div>
        <v-timeline density="compact" side="end" class="mb-3">
          <v-timeline-item
            v-for="(h, i) in detail.history"
            :key="i"
            :dot-color="h.result === 'success' ? 'success' : h.result === 'partial' ? 'warning' : 'error'"
            size="x-small"
          >
            <div class="text-body-2"><strong>{{ h.action }}</strong> — {{ h.result }} ({{ h.duration_ms }} ms)</div>
            <div v-if="h.message" class="text-caption text-medium-emphasis">{{ h.message }}</div>
          </v-timeline-item>
        </v-timeline>
      </template>

      <div class="d-flex flex-wrap ga-2 mt-2">
        <v-btn
          v-if="['pending', 'processing', 'retrying'].includes(job.status)"
          size="small"
          color="error"
          variant="tonal"
          :loading="busy"
          @click="cancel"
        >Cancel</v-btn>
        <v-btn v-if="['failed', 'partial'].includes(job.status)" size="small" color="primary" variant="tonal" :loading="busy" @click="retry">Retry</v-btn>
        <v-btn v-if="job.type !== 'parent'" size="small" variant="tonal" :loading="busy" @click="verify">Verify</v-btn>
        <v-btn v-if="job.type !== 'parent'" size="small" variant="tonal" :loading="busy" @click="rollback">Rollback</v-btn>
      </div>
    </div>
  </v-navigation-drawer>
</template>
