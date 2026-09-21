<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { useJobs } from '@/stores/jobs'
import { useLive } from '@/stores/live'
import JobDrawer from '@/components/JobDrawer.vue'
import type { Job } from '@/api/types'

const store = useJobs()
const live = useLive()
const drawer = ref(false)
const selected = ref<Job | null>(null)
const status = ref<string | null>(null)
const jobType = ref<string | null>(null)

let release: (() => void) | null = null

onMounted(() => {
  void store.list()
  release = live.connect()
})
onUnmounted(() => release?.())

function reload(): void {
  void store.list({ status: status.value ?? undefined, job_type: jobType.value ?? undefined })
}

function open(j: Job): void {
  selected.value = j
  drawer.value = true
}

const statusColor: Record<string, string> = {
  completed: 'success',
  failed: 'error',
  partial: 'warning',
  processing: 'info',
  retrying: 'warning',
  cancelled: 'grey',
  pending: 'grey',
}
const STATUSES = ['pending', 'processing', 'completed', 'failed', 'partial', 'retrying', 'cancelled']
const TYPES = ['parent', 'child', 'direct']
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h5">Deployment jobs</h1>
      <v-chip v-if="live.connected" size="x-small" color="success" variant="tonal" class="ms-3">live</v-chip>
      <v-spacer />
      <v-select v-model="status" :items="STATUSES" label="Status" density="compact" clearable hide-details style="max-width: 160px" class="me-2" @update:model-value="reload" />
      <v-select v-model="jobType" :items="TYPES" label="Type" density="compact" clearable hide-details style="max-width: 140px" class="me-2" @update:model-value="reload" />
      <v-btn variant="text" icon="mdi-refresh" @click="reload" />
    </div>
    <v-alert v-if="store.error" type="error" variant="tonal" density="compact" class="mb-3">{{ store.error }}</v-alert>
    <v-table data-test="jobs-table">
      <thead>
        <tr><th>Certificate</th><th>Type</th><th>Trigger</th><th>Status</th><th>Progress</th><th>Created</th></tr>
      </thead>
      <tbody>
        <tr v-for="j in store.items" :key="j.id" class="cursor-pointer" :data-test="'job-row-' + j.id" @click="open(j)">
          <td>{{ j.certificate_id }}</td>
          <td><v-chip size="x-small" variant="tonal">{{ j.type }}</v-chip></td>
          <td class="text-medium-emphasis">{{ j.triggered_by }}</td>
          <td><v-chip size="x-small" :color="statusColor[j.status]" variant="flat">{{ j.status }}</v-chip></td>
          <td style="min-width: 120px"><v-progress-linear :model-value="j.progress" height="6" rounded :color="statusColor[j.status]" /></td>
          <td class="text-medium-emphasis">{{ new Date(j.created_at).toLocaleString() }}</td>
        </tr>
        <tr v-if="!store.items.length && !store.loading">
          <td colspan="6" class="text-medium-emphasis">No jobs yet.</td>
        </tr>
      </tbody>
    </v-table>
    <JobDrawer v-model="drawer" :job="selected" @changed="reload" />
  </div>
</template>

<style scoped>
.cursor-pointer { cursor: pointer; }
</style>
