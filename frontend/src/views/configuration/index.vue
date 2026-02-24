<script lang="ts" setup>
import type { VxeGridProps } from 'shell/adapter/vxe-table';

import { computed, h, ref } from 'vue';

import { Page, useVbenDrawer, type VbenFormProps } from 'shell/vben/common-ui';
import { LucideEye, LucidePencil, LucideTrash } from 'shell/vben/icons';

import { notification } from 'ant-design-vue';

import { useVbenVxeGrid } from 'shell/adapter/vxe-table';
import { type TargetConfiguration } from '../../api/services';
import { $t } from 'shell/locales';
import { useDeployerConfigurationStore } from '../../stores/deployer-configuration.state';
import { useTenantStore } from 'shell/stores';

import ConfigurationDrawer from './configuration-drawer.vue';

const configStore = useDeployerConfigurationStore();
const tenantStore = useTenantStore();

const tenantOptions = ref<Array<{ label: string; value: number }>>([]);

const statusList = computed(() => [
  {
    value: 'CONFIG_STATUS_ACTIVE',
    label: $t('deployer.enum.configStatus.active'),
  },
  {
    value: 'CONFIG_STATUS_INACTIVE',
    label: $t('deployer.enum.configStatus.inactive'),
  },
  {
    value: 'CONFIG_STATUS_ERROR',
    label: $t('deployer.enum.configStatus.error'),
  },
]);

function statusToColor(status: string | undefined) {
  switch (status) {
    case 'CONFIG_STATUS_ACTIVE':
      return '#52C41A'; // green
    case 'CONFIG_STATUS_INACTIVE':
      return '#8C8C8C'; // gray
    case 'CONFIG_STATUS_ERROR':
      return '#FF4D4F'; // red
    default:
      return '#C9CDD4';
  }
}

function statusToName(status: string | undefined) {
  const item = statusList.value.find((s) => s.value === status);
  return item?.label ?? status ?? '';
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
      label: $t('deployer.page.configuration.tenant'),
      componentProps: {
        options: tenantOptions,
        placeholder: $t('ui.placeholder.select'),
        allowClear: true,
      },
    },
    {
      component: 'Input',
      fieldName: 'providerType',
      label: $t('deployer.page.configuration.providerType'),
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
  ],
};

const gridOptions: VxeGridProps<TargetConfiguration> = {
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
        const resp = await configStore.listConfigurations(
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
    { title: $t('deployer.page.configuration.name'), field: 'name', minWidth: 150 },
    {
      title: $t('deployer.page.configuration.providerType'),
      field: 'providerType',
      width: 120,
    },
    {
      title: $t('ui.table.status'),
      field: 'status',
      slots: { default: 'status' },
      width: 100,
    },
    {
      title: $t('deployer.page.configuration.lastDeploymentAt'),
      field: 'lastDeploymentAt',
      formatter: 'formatDateTime',
      width: 160,
    },
    {
      title: $t('ui.table.description'),
      field: 'description',
      minWidth: 200,
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

const drawerMode = ref<'create' | 'edit' | 'view'>('view');

const [Drawer, drawerApi] = useVbenDrawer({
  connectedComponent: ConfigurationDrawer,

  onOpenChange(isOpen: boolean) {
    if (!isOpen) {
      gridApi.query();
    }
  },
});

function openDrawer(
  row: TargetConfiguration,
  mode: 'create' | 'edit' | 'view',
) {
  drawerMode.value = mode;
  drawerApi.setData({ row, mode });
  drawerApi.open();
}

/* View details */
function handleView(row: TargetConfiguration) {
  openDrawer(row, 'view');
}

/* Edit configuration */
function handleEdit(row: TargetConfiguration) {
  openDrawer(row, 'edit');
}

/* Create new configuration */
function handleCreate() {
  openDrawer({} as TargetConfiguration, 'create');
}

/* Delete configuration */
async function handleDelete(row: TargetConfiguration) {
  if (!row.id) {
    notification.error({
      message: $t('deployer.page.configuration.error.nameRequired'),
    });
    return;
  }

  try {
    await configStore.deleteConfiguration(row.id);
    notification.success({ message: $t('deployer.page.configuration.deleteSuccess') });
    await gridApi.query();
  } catch {
    notification.error({ message: $t('ui.notification.delete_failed') });
  }
}
</script>

<template>
  <Page auto-content-height>
    <Grid :table-title="$t('deployer.menu.configuration')">
      <template #toolbar-tools>
        <a-button class="mr-2" type="primary" @click="handleCreate">
          {{ $t('ui.button.create', { moduleName: '' }) }}
        </a-button>
      </template>
      <template #status="{ row }">
        <a-tag :color="statusToColor(row.status)">
          {{ statusToName(row.status) }}
        </a-tag>
      </template>
      <template #action="{ row }">
        <a-button
          type="link"
          :icon="h(LucideEye)"
          :title="$t('ui.button.view')"
          @click.stop="handleView(row)"
        />
        <a-button
          type="link"
          :icon="h(LucidePencil)"
          :title="$t('ui.button.edit')"
          @click.stop="handleEdit(row)"
        />
        <a-popconfirm
          :cancel-text="$t('ui.button.cancel')"
          :ok-text="$t('ui.button.ok')"
          :title="$t('deployer.page.configuration.confirmDelete')"
          @confirm="handleDelete(row)"
        >
          <a-button
            danger
            type="link"
            :icon="h(LucideTrash)"
            :title="$t('ui.button.delete', { moduleName: '' })"
          />
        </a-popconfirm>
      </template>
    </Grid>
    <Drawer />
  </Page>
</template>
