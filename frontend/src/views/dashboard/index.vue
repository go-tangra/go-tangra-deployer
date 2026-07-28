<script lang="ts" setup>
import { computed, onMounted, onUnmounted, ref } from 'vue';

import { Page } from 'shell/vben/common-ui';
import { $t } from 'shell/locales';

import {
  Card,
  Col,
  Empty,
  Progress,
  Row,
  Spin,
  Statistic,
  Tag,
} from 'ant-design-vue';

import {
  DeployerStatisticsService,
  type GetStatisticsResponse,
} from '../../api/services';
import { formatDateTime, formatTime } from '../../datetime';

// 30s auto-refresh: dashboards lose value if they go stale during an
// active incident, but we don't need second-by-second polling either.
// Operators can hit the Refresh button for an immediate fetch.
const REFRESH_INTERVAL_MS = 30_000;

const stats = ref<GetStatisticsResponse | null>(null);
const loading = ref(true);
const lastUpdated = ref<Date | null>(null);
let refreshTimer: ReturnType<typeof setInterval> | undefined;

async function load() {
  loading.value = true;
  try {
    stats.value = await DeployerStatisticsService.getStatistics();
    lastUpdated.value = new Date();
  } catch {
    // Silent: the empty state covers the failure mode for the operator.
    // Errors are logged on the server side; surfacing a transient blip
    // here would just create noise on the auto-refresh path.
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  load();
  refreshTimer = setInterval(load, REFRESH_INTERVAL_MS);
});

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer);
});

// Number-coerce the API's stringified int64s. They come over the wire as
// strings because JSON has no 64-bit int type and the proto field is
// uint64/int64. The display only cares about the magnitude so a JS
// double is fine for any cert/job count we'll realistically hit.
const num = (v: string | number | undefined): number => Number(v ?? 0);

const jobs = computed(() => stats.value?.jobs);
const targets = computed(() => stats.value?.targets);
const configurations = computed(() => stats.value?.configurations);
const recentErrors = computed(() => stats.value?.recentErrors ?? []);

// Total jobs across all statuses, used to derive proportional bars.
const jobTotal = computed(() => num(jobs.value?.totalCount));

// Job status breakdown — fed into the progress strip so operators see
// in-flight vs completed at a glance without reading 6 numbers.
interface StatusSlice {
  key: string;
  label: string;
  value: number;
  color: string;
}

const jobStatusSlices = computed<StatusSlice[]>(() => {
  const j = jobs.value;
  if (!j) return [];
  return [
    { key: 'completed', label: $t('deployer.enum.jobStatus.completed'), value: num(j.completedCount), color: '#52c41a' },
    { key: 'processing', label: $t('deployer.enum.jobStatus.processing'), value: num(j.processingCount), color: '#1677ff' },
    { key: 'pending', label: $t('deployer.enum.jobStatus.pending'), value: num(j.pendingCount), color: '#faad14' },
    { key: 'retrying', label: $t('deployer.enum.jobStatus.retrying'), value: num(j.retryingCount), color: '#722ed1' },
    { key: 'failed', label: $t('deployer.enum.jobStatus.failed'), value: num(j.failedCount), color: '#ff4d4f' },
    { key: 'cancelled', label: $t('deployer.enum.jobStatus.cancelled'), value: num(j.cancelledCount), color: '#8c8c8c' },
  ].filter((s) => s.value > 0);
});

const successRate24h = computed(() => {
  const r = jobs.value?.last24Hours?.successRate;
  return r === undefined ? null : Math.round(r * 10) / 10;
});

const successRate7d = computed(() => {
  const r = jobs.value?.last7Days?.successRate;
  return r === undefined ? null : Math.round(r * 10) / 10;
});

const providerEntries = computed<Array<[string, number]>>(() => {
  const m = configurations.value?.byProviderType ?? {};
  return Object.entries(m)
    .map(([k, v]) => [k, num(v)] as [string, number])
    .filter(([, v]) => v > 0)
    .sort((a, b) => b[1] - a[1]);
});

const providerMax = computed(() => {
  const entries = providerEntries.value;
  return entries.length === 0 ? 0 : Math.max(...entries.map(([, v]) => v));
});

const triggerEntries = computed<Array<[string, number]>>(() => {
  const m = jobs.value?.byTriggerType ?? {};
  return Object.entries(m)
    .map(([k, v]) => [k, num(v)] as [string, number])
    .filter(([, v]) => v > 0)
    .sort((a, b) => b[1] - a[1]);
});

function triggerLabel(code: string): string {
  switch (code) {
    case 'TRIGGER_TYPE_MANUAL':
      return $t('deployer.enum.triggerType.manual');
    case 'TRIGGER_TYPE_AUTO_RENEWAL':
      return $t('deployer.enum.triggerType.autoRenewal');
    case 'TRIGGER_TYPE_EVENT':
      return $t('deployer.enum.triggerType.event');
    default:
      return code;
  }
}

const lastUpdatedLabel = computed(() => {
  if (!lastUpdated.value) return '';
  return formatTime(lastUpdated.value);
});
</script>

<template>
  <Page :title="$t('deployer.page.dashboard.title')" :description="$t('deployer.page.dashboard.description')">
    <template #extra>
      <span v-if="lastUpdatedLabel" class="text-xs text-gray-500 mr-3">
        {{ $t('deployer.page.dashboard.lastUpdated') }}: {{ lastUpdatedLabel }}
      </span>
      <a class="ant-btn ant-btn-default" @click="load">{{ $t('ui.button.refresh') }}</a>
    </template>

    <Spin :spinning="loading && !stats">
      <Empty
        v-if="!loading && !stats"
        :description="$t('deployer.page.dashboard.empty')"
        class="py-12"
      />

      <template v-if="stats">
        <!-- KPI row -->
        <Row :gutter="[16, 16]">
          <Col :xs="24" :sm="12" :md="6">
            <Card>
              <Statistic
                :title="$t('deployer.page.dashboard.targetsTotal')"
                :value="num(targets?.totalCount)"
              />
              <div class="text-xs text-gray-500 mt-1">
                {{ $t('deployer.page.dashboard.autoDeployEnabled') }}:
                <b>{{ num(targets?.autoDeployEnabledCount) }}</b>
              </div>
            </Card>
          </Col>

          <Col :xs="24" :sm="12" :md="6">
            <Card>
              <Statistic
                :title="$t('deployer.page.dashboard.configurationsTotal')"
                :value="num(configurations?.totalCount)"
              />
              <div class="text-xs text-gray-500 mt-1">
                {{ $t('deployer.page.dashboard.configurationsActive') }}:
                <b>{{ num(configurations?.activeCount) }}</b>
              </div>
            </Card>
          </Col>

          <Col :xs="24" :sm="12" :md="6">
            <Card>
              <Statistic
                :title="$t('deployer.page.dashboard.jobsTotal')"
                :value="jobTotal"
              />
              <div class="text-xs text-gray-500 mt-1">
                <Tag color="processing">
                  {{ $t('deployer.page.dashboard.inFlight') }}:
                  {{ num(jobs?.processingCount) + num(jobs?.pendingCount) + num(jobs?.retryingCount) }}
                </Tag>
              </div>
            </Card>
          </Col>

          <Col :xs="24" :sm="12" :md="6">
            <Card>
              <Statistic
                :title="$t('deployer.page.dashboard.successRate24h')"
                :value="successRate24h ?? 0"
                :suffix="successRate24h === null ? '' : '%'"
                :precision="1"
              />
              <div class="text-xs text-gray-500 mt-1">
                {{ $t('deployer.page.dashboard.last7d') }}:
                <b>{{ successRate7d === null ? '—' : `${successRate7d}%` }}</b>
              </div>
            </Card>
          </Col>
        </Row>

        <!-- Job status breakdown -->
        <Card class="mt-4" :title="$t('deployer.page.dashboard.jobsByStatus')">
          <div v-if="jobStatusSlices.length === 0" class="text-gray-500 text-center py-6">
            {{ $t('deployer.page.dashboard.noJobs') }}
          </div>
          <div v-else class="flex flex-col gap-3">
            <div v-for="slice in jobStatusSlices" :key="slice.key">
              <div class="flex justify-between text-sm mb-1">
                <span>{{ slice.label }}</span>
                <span>
                  <b>{{ slice.value }}</b>
                  <span class="text-gray-400 ml-1">
                    ({{ jobTotal === 0 ? 0 : Math.round((slice.value / jobTotal) * 100) }}%)
                  </span>
                </span>
              </div>
              <Progress
                :percent="jobTotal === 0 ? 0 : (slice.value / jobTotal) * 100"
                :show-info="false"
                :stroke-color="slice.color"
              />
            </div>
          </div>
        </Card>

        <!-- Provider & trigger breakdowns -->
        <Row :gutter="[16, 16]" class="mt-4">
          <Col :xs="24" :md="12">
            <Card :title="$t('deployer.page.dashboard.configsByProvider')">
              <div v-if="providerEntries.length === 0" class="text-gray-500 text-center py-6">
                {{ $t('deployer.page.dashboard.noConfigs') }}
              </div>
              <div v-else class="flex flex-col gap-3">
                <div v-for="[name, value] in providerEntries" :key="name">
                  <div class="flex justify-between text-sm mb-1">
                    <span>{{ name }}</span>
                    <span><b>{{ value }}</b></span>
                  </div>
                  <Progress
                    :percent="providerMax === 0 ? 0 : (value / providerMax) * 100"
                    :show-info="false"
                    stroke-color="#1677ff"
                  />
                </div>
              </div>
            </Card>
          </Col>

          <Col :xs="24" :md="12">
            <Card :title="$t('deployer.page.dashboard.jobsByTrigger')">
              <div v-if="triggerEntries.length === 0" class="text-gray-500 text-center py-6">
                {{ $t('deployer.page.dashboard.noJobs') }}
              </div>
              <div v-else class="flex flex-col gap-2">
                <div v-for="[code, value] in triggerEntries" :key="code" class="flex justify-between items-center py-1">
                  <span>{{ triggerLabel(code) }}</span>
                  <Tag :color="code === 'TRIGGER_TYPE_AUTO_RENEWAL' ? 'green' : 'blue'">{{ value }}</Tag>
                </div>
              </div>
            </Card>
          </Col>
        </Row>

        <!-- Recent errors -->
        <Card class="mt-4" :title="$t('deployer.page.dashboard.recentErrors')">
          <div v-if="recentErrors.length === 0" class="text-gray-500 text-center py-6">
            {{ $t('deployer.page.dashboard.noErrors') }}
          </div>
          <ul v-else class="flex flex-col gap-2">
            <li v-for="e in recentErrors" :key="(e.jobId || '') + (e.occurredAt || '')" class="flex flex-col border-b pb-2">
              <div class="flex justify-between text-sm">
                <span><b>{{ e.configurationName || e.providerType || e.jobId }}</b></span>
                <span class="text-gray-500">{{ e.occurredAt ? formatDateTime(e.occurredAt) : '' }}</span>
              </div>
              <div class="text-xs text-red-600 truncate">{{ e.errorMessage }}</div>
            </li>
          </ul>
        </Card>
      </template>
    </Spin>
  </Page>
</template>
