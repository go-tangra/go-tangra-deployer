<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useTargets } from '@/stores/targets'
import { useConfigurations } from '@/stores/configurations'
import { useJobs } from '@/stores/jobs'
import { useStats } from '@/stores/stats'
import StatsCard from '@/components/StatsCard.vue'

// The /statistics endpoint is a later increment (US5); until then the dashboard
// derives its figures from the tenant's targets, configurations and recent jobs.
const targets = useTargets()
const configs = useConfigurations()
const jobs = useJobs()
const stats = useStats()

onMounted(() => {
  void targets.list()
  void configs.list()
  void jobs.list()
  void stats.load()
})

const autoDeploy = computed(() => targets.items.filter((t) => t.auto_deploy).length)

const byStatus = computed(() => {
  const m: Record<string, number> = {}
  for (const j of jobs.items) m[j.status] = (m[j.status] ?? 0) + 1
  return m
})

const successRate = computed(() => {
  const snap = stats.snapshot
  if (snap) return Math.round(snap.success_rate_24h * 100) + '%'
  const done = jobs.items.filter((j) => ['completed', 'failed', 'partial'].includes(j.status))
  if (!done.length) return '—'
  const ok = done.filter((j) => j.status === 'completed').length
  return Math.round((ok / done.length) * 100) + '%'
})

const recentErrors = computed(() => stats.snapshot?.recent_errors ?? [])

const byProvider = computed(() => {
  const m: Record<string, number> = {}
  for (const c of configs.items) m[c.provider_type] = (m[c.provider_type] ?? 0) + 1
  return Object.entries(m).sort((a, b) => b[1] - a[1])
})

const statusColor: Record<string, string> = {
  completed: 'success',
  failed: 'error',
  partial: 'warning',
  processing: 'info',
  retrying: 'warning',
  cancelled: 'grey',
  pending: 'grey',
}
</script>

<template>
  <div>
    <h1 class="text-h5 mb-4">Deployer</h1>
    <v-row>
      <v-col cols="12" sm="6" md="3">
        <StatsCard title="Targets" :value="targets.items.length" icon="mdi-target" color="primary" />
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <StatsCard title="Configurations" :value="configs.items.length" icon="mdi-cog-outline" color="info" />
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <StatsCard title="Auto-deploy targets" :value="autoDeploy" icon="mdi-autorenew" color="success" />
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <StatsCard title="Recent success rate" :value="successRate" icon="mdi-check-decagram" color="teal" subtitle="last 24h" />
      </v-col>
    </v-row>

    <v-row class="mt-2">
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title class="text-subtitle-1">Jobs by status</v-card-title>
          <v-card-text>
            <div v-for="(n, s) in byStatus" :key="s" class="d-flex align-center mb-2">
              <v-chip size="x-small" :color="statusColor[s]" variant="flat" class="me-3" style="min-width: 92px; justify-content: center">{{ s }}</v-chip>
              <v-progress-linear :model-value="jobs.items.length ? (n / jobs.items.length) * 100 : 0" height="8" rounded :color="statusColor[s]" />
              <span class="ms-3 text-body-2">{{ n }}</span>
            </div>
            <div v-if="!jobs.items.length" class="text-medium-emphasis">No jobs yet.</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title class="text-subtitle-1">Configurations by provider</v-card-title>
          <v-card-text>
            <div v-for="[prov, n] in byProvider" :key="prov" class="d-flex align-center mb-2">
              <v-chip size="x-small" variant="tonal" class="me-3" style="min-width: 92px; justify-content: center">{{ prov }}</v-chip>
              <v-progress-linear :model-value="configs.items.length ? (n / configs.items.length) * 100 : 0" height="8" rounded color="primary" />
              <span class="ms-3 text-body-2">{{ n }}</span>
            </div>
            <div v-if="!byProvider.length" class="text-medium-emphasis">No configurations yet.</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-row v-if="recentErrors.length" class="mt-2">
      <v-col cols="12">
        <v-card>
          <v-card-title class="text-subtitle-1">Recent errors</v-card-title>
          <v-card-text>
            <v-table density="compact">
              <thead>
                <tr><th>When</th><th>Certificate</th><th>Message</th></tr>
              </thead>
              <tbody>
                <tr v-for="e in recentErrors" :key="e.job_id">
                  <td class="text-medium-emphasis">{{ new Date(e.at).toLocaleString() }}</td>
                  <td>{{ e.certificate_id }}</td>
                  <td>{{ e.message }}</td>
                </tr>
              </tbody>
            </v-table>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>
