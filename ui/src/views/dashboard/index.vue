<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useTargets } from '@/stores/targets'
import { useConfigurations } from '@/stores/configurations'
import { useJobs } from '@/stores/jobs'
import { useStats } from '@/stores/stats'
import { UiPage, UiCard, UiStatGrid, UiStatTile, UiBarList, UiDataTable, type BarItem, type Column } from '@go-tangra/ui'

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

const statusColor: Record<string, NonNullable<BarItem['color']>> = { completed: 'success', failed: 'error', partial: 'warning', processing: 'info', retrying: 'warning', cancelled: 'neutral', pending: 'neutral' }
const statusBars = computed<BarItem[]>(() => Object.entries(byStatus.value).sort((a, b) => b[1] - a[1]).map(([label, value]) => ({ label, value, color: statusColor[label] ?? 'primary' })))
const providerBars = computed<BarItem[]>(() => byProvider.value.map(([label, value]) => ({ label, value, color: 'primary' })))
const errorRows = computed(() => recentErrors.value.map((e) => ({ ...e, id: e.job_id })))
const errorColumns: Column<(typeof errorRows.value)[number]>[] = [{ key: 'at', label: 'When', format: (e) => new Date(e.at).toLocaleString() }, { key: 'certificate_id', label: 'Certificate' }, { key: 'message', label: 'Message' }]
</script>
<template>
  <UiPage title="Deployer">
    <UiStatGrid class="mb-4" :cols="4">
      <UiStatTile title="Targets" :value="targets.items.length" icon="mdi-target" color="primary" />
      <UiStatTile title="Configurations" :value="configs.items.length" icon="mdi-cog-outline" color="info" />
      <UiStatTile title="Auto-deploy targets" :value="autoDeploy" icon="mdi-autorenew" color="success" />
      <UiStatTile title="Recent success rate" :value="successRate" icon="mdi-check-decagram" color="accent" subtitle="last 24h" />
    </UiStatGrid>
    <div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <UiCard title="Jobs by status"><UiBarList :items="statusBars" empty-title="No jobs yet" /></UiCard>
      <UiCard title="Configurations by provider"><UiBarList :items="providerBars" empty-title="No configurations yet" /></UiCard>
      <UiCard v-if="errorRows.length" title="Recent errors" class="lg:col-span-2" :padded="false"><UiDataTable :items="errorRows" :columns="errorColumns" caption="Recent errors" /></UiCard>
    </div>
  </UiPage>
</template>
