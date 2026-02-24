<script lang="ts" setup>
import { computed, ref } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';

import {
  Descriptions,
  DescriptionsItem,
  Divider,
  Progress,
  Table,
  Tag,
} from 'ant-design-vue';

import {
  type DeploymentJob,
  type JobHistoryEntry,
} from '../../api/services';
import { $t } from 'shell/locales';
import { useDeployerJobStore } from '../../stores/deployer-job.state';

const jobStore = useDeployerJobStore();

const data = ref<{ row: DeploymentJob }>();
// History entries from the API - using 'any' because the API returns durationMs as string
// but the typed interface has it as number
const history = ref<Array<JobHistoryEntry & { durationMs?: string | number }>>([]);
const loading = ref(false);

function statusToColor(status: string | undefined) {
  switch (status) {
    case 'JOB_STATUS_COMPLETED':
      return '#52C41A'; // green
    case 'JOB_STATUS_PROCESSING':
      return '#1890FF'; // blue
    case 'JOB_STATUS_PENDING':
      return '#FAAD14'; // orange
    case 'JOB_STATUS_RETRYING':
      return '#FAAD14'; // orange
    case 'JOB_STATUS_FAILED':
      return '#FF4D4F'; // red
    case 'JOB_STATUS_CANCELLED':
      return '#8C8C8C'; // gray
    default:
      return '#C9CDD4';
  }
}

function statusToName(status: string | undefined) {
  switch (status) {
    case 'JOB_STATUS_COMPLETED':
      return $t('deployer.enum.jobStatus.completed');
    case 'JOB_STATUS_PROCESSING':
      return $t('deployer.enum.jobStatus.processing');
    case 'JOB_STATUS_PENDING':
      return $t('deployer.enum.jobStatus.pending');
    case 'JOB_STATUS_RETRYING':
      return $t('deployer.enum.jobStatus.retrying');
    case 'JOB_STATUS_FAILED':
      return $t('deployer.enum.jobStatus.failed');
    case 'JOB_STATUS_CANCELLED':
      return $t('deployer.enum.jobStatus.cancelled');
    default:
      return status ?? '';
  }
}

function triggerToName(trigger: string | undefined) {
  switch (trigger) {
    case 'TRIGGER_TYPE_MANUAL':
      return $t('deployer.enum.triggerType.manual');
    case 'TRIGGER_TYPE_EVENT':
      return $t('deployer.enum.triggerType.event');
    case 'TRIGGER_TYPE_AUTO_RENEWAL':
      return $t('deployer.enum.triggerType.autoRenewal');
    default:
      return trigger ?? '';
  }
}

function resultToColor(result: string | undefined) {
  switch (result) {
    case 'RESULT_SUCCESS':
      return '#52C41A';
    case 'RESULT_FAILURE':
      return '#FF4D4F';
    case 'RESULT_PARTIAL':
      return '#FAAD14';
    default:
      return '#C9CDD4';
  }
}

function resultToName(result: string | undefined) {
  switch (result) {
    case 'RESULT_SUCCESS':
      return $t('deployer.enum.historyResult.success');
    case 'RESULT_FAILURE':
      return $t('deployer.enum.historyResult.failure');
    case 'RESULT_PARTIAL':
      return $t('deployer.enum.historyResult.partial');
    default:
      return result ?? '';
  }
}

function actionToName(action: string | undefined) {
  switch (action) {
    case 'ACTION_DEPLOY':
      return $t('deployer.enum.historyAction.deploy');
    case 'ACTION_ROLLBACK':
      return $t('deployer.enum.historyAction.rollback');
    case 'ACTION_VERIFY':
      return $t('deployer.enum.historyAction.verify');
    default:
      return action ?? '';
  }
}

function formatDateTime(value: string | undefined) {
  if (!value) return '-';
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

async function loadJobResult(jobId: string) {
  loading.value = true;
  try {
    const resp = await jobStore.getJobResult(jobId);
    history.value = (resp.history ?? []) as Array<JobHistoryEntry & { durationMs?: string | number }>;
  } catch (e) {
    console.error('Failed to load job result:', e);
  } finally {
    loading.value = false;
  }
}

const historyColumns = [
  {
    title: $t('deployer.page.job.historyEntry.action'),
    dataIndex: 'action',
    key: 'action',
    customRender: ({ value }: { value: string }) => actionToName(value),
  },
  {
    title: $t('deployer.page.job.historyEntry.result'),
    dataIndex: 'result',
    key: 'result',
  },
  {
    title: $t('deployer.page.job.historyEntry.message'),
    dataIndex: 'message',
    key: 'message',
    ellipsis: true,
  },
  {
    title: $t('deployer.page.job.historyEntry.durationMs'),
    dataIndex: 'durationMs',
    key: 'durationMs',
    width: 120,
  },
  {
    title: $t('deployer.page.job.historyEntry.createTime'),
    dataIndex: 'createTime',
    key: 'createTime',
    customRender: ({ value }: { value: string }) => formatDateTime(value),
    width: 160,
  },
];

const [Drawer, drawerApi] = useVbenDrawer({
  onCancel() {
    drawerApi.close();
  },

  async onOpenChange(isOpen) {
    if (isOpen) {
      data.value = drawerApi.getData() as {
        row: DeploymentJob;
      };
      if (data.value?.row.id) {
        await loadJobResult(data.value.row.id);
      }
    } else {
      history.value = [];
    }
  },
});

const job = computed(() => data.value?.row);
</script>

<template>
  <Drawer :title="$t('deployer.page.job.details')" :footer="false">
    <template v-if="job">
      <Descriptions :column="1" bordered size="small">
        <DescriptionsItem :label="$t('deployer.page.job.id')">
          {{ job.id }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.deploymentTargetName')">
          {{ job.deploymentTargetName || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.targetConfigurationName')">
          {{ job.targetConfigurationName || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.certificateId')">
          {{ job.certificateId }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.certificateSerial')">
          {{ job.certificateSerial ?? '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.status')">
          <Tag :color="statusToColor(job.status)">
            {{ statusToName(job.status) }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.statusMessage')">
          {{ job.statusMessage ?? '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.progress')">
          <Progress
            :percent="job.progress ?? 0"
            :status="
              job.status === 'JOB_STATUS_FAILED'
                ? 'exception'
                : job.status === 'JOB_STATUS_COMPLETED'
                  ? 'success'
                  : 'active'
            "
            size="small"
          />
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.triggeredBy')">
          <Tag color="default">
            {{ triggerToName(job.triggeredBy) }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.retryCount')">
          {{ job.retryCount ?? 0 }} / {{ job.maxRetries ?? 0 }}
        </DescriptionsItem>
      </Descriptions>

      <Divider>{{ $t('ui.table.timestamps') }}</Divider>
      <Descriptions :column="2" bordered size="small">
        <DescriptionsItem :label="$t('deployer.page.job.startedAt')">
          {{ formatDateTime(job.startedAt) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.completedAt')">
          {{ formatDateTime(job.completedAt) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.job.nextRetryAt')">
          {{ formatDateTime(job.nextRetryAt) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.createdAt')">
          {{ formatDateTime(job.createTime) }}
        </DescriptionsItem>
      </Descriptions>

      <Divider>{{ $t('deployer.page.job.history') }}</Divider>
      <Table
        :columns="historyColumns"
        :data-source="history"
        :loading="loading"
        :pagination="false"
        size="small"
        row-key="id"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'result'">
            <Tag :color="resultToColor(record.result)">
              {{ resultToName(record.result) }}
            </Tag>
          </template>
        </template>
      </Table>
    </template>
  </Drawer>
</template>
