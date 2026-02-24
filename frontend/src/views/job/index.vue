<script lang="ts" setup>
import type { VxeGridProps } from 'shell/adapter/vxe-table';

import { computed, h, ref } from 'vue';

import { Page, useVbenDrawer, type VbenFormProps } from 'shell/vben/common-ui';
import {
  LucideEye,
  LucideRefreshCw,
  LucideX,
} from 'shell/vben/icons';

import { notification, Progress } from 'ant-design-vue';

import { useVbenVxeGrid } from 'shell/adapter/vxe-table';
import { type DeploymentJob } from '../../api/services';
import { $t } from 'shell/locales';
import { useDeployerJobStore } from '../../stores/deployer-job.state';
import { useTenantStore } from 'shell/stores';

import JobDrawer from './job-drawer.vue';

const jobStore = useDeployerJobStore();
const tenantStore = useTenantStore();

const tenantOptions = ref<Array<{ label: string; value: number }>>([]);

const statusList = computed(() => [
  { value: 'JOB_STATUS_PENDING', label: $t('deployer.enum.jobStatus.pending') },
  {
    value: 'JOB_STATUS_PROCESSING',
    label: $t('deployer.enum.jobStatus.processing'),
  },
  {
    value: 'JOB_STATUS_COMPLETED',
    label: $t('deployer.enum.jobStatus.completed'),
  },
  { value: 'JOB_STATUS_FAILED', label: $t('deployer.enum.jobStatus.failed') },
  {
    value: 'JOB_STATUS_CANCELLED',
    label: $t('deployer.enum.jobStatus.cancelled'),
  },
  {
    value: 'JOB_STATUS_RETRYING',
    label: $t('deployer.enum.jobStatus.retrying'),
  },
]);

const triggerList = computed(() => [
  { value: 'TRIGGER_TYPE_MANUAL', label: $t('deployer.enum.triggerType.manual') },
  { value: 'TRIGGER_TYPE_EVENT', label: $t('deployer.enum.triggerType.event') },
  {
    value: 'TRIGGER_TYPE_AUTO_RENEWAL',
    label: $t('deployer.enum.triggerType.autoRenewal'),
  },
]);

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
  const item = statusList.value.find((s) => s.value === status);
  return item?.label ?? status ?? '';
}

function triggerToName(trigger: string | undefined) {
  const item = triggerList.value.find((t) => t.value === trigger);
  return item?.label ?? trigger ?? '';
}

async function loadTenants() {
  const result = await tenantStore.listTenant(
    { page: 1, pageSize: 100 },
    null,
    'id,name,code',
  );
  tenantOptions.value = (result.items ?? []).map((tenant: any) => ({
    value: tenant.id,
    label: `${tenant.name} (${tenant.code})`,
  }));
}

loadTenants();

const formOptions: VbenFormProps = {
  collapsed: false,
  showCollapseButton: false,
  submitOnEnter: true,
  schema: [
    {
      component: 'Select',
      fieldName: 'tenantId',
      label: $t('deployer.page.job.tenant'),
      componentProps: {
        options: tenantOptions,
        placeholder: $t('ui.placeholder.select'),
        allowClear: true,
      },
    },
    {
      component: 'Input',
      fieldName: 'targetId',
      label: $t('deployer.page.job.targetId'),
      componentProps: {
        placeholder: $t('ui.placeholder.input'),
        allowClear: true,
      },
    },
    {
      component: 'Select',
      fieldName: 'status',
      label: $t('ui.table.status'),
      componentProps: {
        options: statusList,
        placeholder: $t('ui.placeholder.select'),
        allowClear: true,
      },
    },
    {
      component: 'Select',
      fieldName: 'triggeredBy',
      label: $t('deployer.page.job.triggeredBy'),
      componentProps: {
        options: triggerList,
        placeholder: $t('ui.placeholder.select'),
        allowClear: true,
      },
    },
  ],
};

const gridOptions: VxeGridProps<DeploymentJob> = {
  height: 'auto',
  stripe: false,
  toolbarConfig: {
    custom: true,
    export: true,
    import: false,
    refresh: true,
    zoom: true,
  },
  exportConfig: {},
  rowConfig: {
    isHover: true,
  },
  pagerConfig: {
    enabled: true,
    pageSize: 20,
    pageSizes: [10, 20, 50, 100],
  },

  proxyConfig: {
    ajax: {
      query: async ({ page }, formValues) => {
        const resp = await jobStore.listJobs(
          { page: page.currentPage, pageSize: page.pageSize },
          formValues,
        );
        return {
          items: resp.items ?? [],
          total: resp.total ?? 0,
        };
      },
    },
  },

  columns: [
    { title: $t('ui.table.seq'), type: 'seq', width: 50 },
    { title: $t('deployer.page.job.targetName'), field: 'targetName', minWidth: 150 },
    {
      title: $t('deployer.page.job.certificateSerial'),
      field: 'certificateSerial',
      width: 200,
    },
    {
      title: $t('ui.table.status'),
      field: 'status',
      slots: { default: 'status' },
      width: 100,
    },
    {
      title: $t('deployer.page.job.progress'),
      field: 'progress',
      slots: { default: 'progress' },
      width: 120,
    },
    {
      title: $t('deployer.page.job.triggeredBy'),
      field: 'triggeredBy',
      slots: { default: 'triggeredBy' },
      width: 100,
    },
    {
      title: $t('deployer.page.job.retryCount'),
      field: 'retryCount',
      width: 100,
    },
    {
      title: $t('ui.table.createdAt'),
      field: 'createTime',
      formatter: 'formatDateTime',
      width: 160,
    },
    {
      title: $t('ui.table.action'),
      field: 'action',
      fixed: 'right',
      slots: { default: 'action' },
      width: 120,
    },
  ],
};

const [Grid, gridApi] = useVbenVxeGrid({ gridOptions, formOptions });

const [Drawer, drawerApi] = useVbenDrawer({
  connectedComponent: JobDrawer,

  onOpenChange(isOpen: boolean) {
    if (!isOpen) {
      gridApi.query();
    }
  },
});

/* View details */
function handleView(row: DeploymentJob) {
  drawerApi.setData({ row });
  drawerApi.open();
}

/* Cancel job */
async function handleCancel(row: DeploymentJob) {
  if (!row.id) return;

  try {
    await jobStore.cancelJob(row.id);
    notification.success({ message: $t('deployer.page.job.cancelSuccess') });
    await gridApi.query();
  } catch {
    notification.error({ message: $t('ui.notification.operation_failed') });
  }
}

/* Retry job */
async function handleRetry(row: DeploymentJob) {
  if (!row.id) return;

  try {
    await jobStore.retryJob(row.id);
    notification.success({ message: $t('deployer.page.job.retrySuccess') });
    await gridApi.query();
  } catch {
    notification.error({ message: $t('ui.notification.operation_failed') });
  }
}

function canCancel(status: string | undefined) {
  return status === 'JOB_STATUS_PENDING' || status === 'JOB_STATUS_PROCESSING';
}

function canRetry(status: string | undefined) {
  return status === 'JOB_STATUS_FAILED';
}
</script>

<template>
  <Page auto-content-height>
    <Grid :table-title="$t('deployer.menu.job')">
      <template #status="{ row }">
        <a-tag :color="statusToColor(row.status)">
          {{ statusToName(row.status) }}
        </a-tag>
      </template>
      <template #progress="{ row }">
        <Progress
          :percent="row.progress ?? 0"
          :status="
            row.status === 'JOB_STATUS_FAILED'
              ? 'exception'
              : row.status === 'JOB_STATUS_COMPLETED'
                ? 'success'
                : 'active'
          "
          size="small"
        />
      </template>
      <template #triggeredBy="{ row }">
        <a-tag color="default">
          {{ triggerToName(row.triggeredBy) }}
        </a-tag>
      </template>
      <template #action="{ row }">
        <a-button
          type="link"
          :icon="h(LucideEye)"
          :title="$t('ui.button.view')"
          @click.stop="handleView(row)"
        />
        <a-popconfirm
          v-if="canCancel(row.status)"
          :cancel-text="$t('ui.button.cancel')"
          :ok-text="$t('ui.button.ok')"
          :title="$t('deployer.page.job.confirmCancel')"
          @confirm="handleCancel(row)"
        >
          <a-button
            danger
            type="link"
            :icon="h(LucideX)"
            :title="$t('deployer.page.job.cancel')"
          />
        </a-popconfirm>
        <a-popconfirm
          v-if="canRetry(row.status)"
          :cancel-text="$t('ui.button.cancel')"
          :ok-text="$t('ui.button.ok')"
          :title="$t('deployer.page.job.confirmRetry')"
          @confirm="handleRetry(row)"
        >
          <a-button
            type="link"
            :icon="h(LucideRefreshCw)"
            :title="$t('deployer.page.job.retry')"
          />
        </a-popconfirm>
      </template>
    </Grid>
    <Drawer />
  </Page>
</template>
