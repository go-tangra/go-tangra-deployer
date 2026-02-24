<script lang="ts" setup>
import type { VxeGridProps } from 'shell/adapter/vxe-table';

import { h, ref } from 'vue';

import { Page, useVbenDrawer, type VbenFormProps } from 'shell/vben/common-ui';
import { LucideEye, LucidePencil,  LucideTrash } from 'shell/vben/icons';

import { notification } from 'ant-design-vue';

import { useVbenVxeGrid } from 'shell/adapter/vxe-table';
import { type DeploymentTarget } from '../../api/services';
import { $t } from 'shell/locales';
import { useDeployerTargetStore } from '../../stores/deployer-target.state';
import { useTenantStore } from 'shell/stores';

import TargetDrawer from './target-drawer.vue';

const targetStore = useDeployerTargetStore();
const tenantStore = useTenantStore();

const tenantOptions = ref<Array<{ label: string; value: number }>>([]);


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
      label: $t('deployer.page.target.tenant'),
      componentProps: {
        options: tenantOptions,
        placeholder: $t('ui.placeholder.select'),
        allowClear: true,
      },
    },
    {
      component: 'Select',
      fieldName: 'autoDeployOnRenewal',
      label: $t('deployer.page.target.autoDeployOnRenewal'),
      componentProps: {
        options: [
          { value: true, label: $t('enum.enable.true') },
          { value: false, label: $t('enum.enable.false') },
        ],
        placeholder: $t('ui.placeholder.select'),
        allowClear: true,
      },
    },
  ],
};

const gridOptions: VxeGridProps<DeploymentTarget> = {
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
        const resp = await targetStore.listTargets(
          { page: page.currentPage, pageSize: page.pageSize },
          { ...formValues, includeConfigurations: true },
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
    { title: $t('deployer.page.target.name'), field: 'name', minWidth: 150 },
    {
      title: $t('deployer.page.target.configurationCount'),
      field: 'configurationCount',
      width: 120,
    },
    {
      title: $t('deployer.page.target.autoDeployOnRenewal'),
      field: 'autoDeployOnRenewal',
      slots: { default: 'autoDeploy' },
      width: 120,
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
  connectedComponent: TargetDrawer,

  onOpenChange(isOpen: boolean) {
    if (!isOpen) {
      gridApi.query();
    }
  },
});

function openDrawer(
  row: DeploymentTarget,
  mode: 'create' | 'edit' | 'view',
) {
  drawerMode.value = mode;
  drawerApi.setData({ row, mode });
  drawerApi.open();
}

/* View details */
function handleView(row: DeploymentTarget) {
  openDrawer(row, 'view');
}

/* Edit target */
function handleEdit(row: DeploymentTarget) {
  openDrawer(row, 'edit');
}

/* Create new target */
function handleCreate() {
  openDrawer({} as DeploymentTarget, 'create');
}

/* Delete target */
async function handleDelete(row: DeploymentTarget) {
  if (!row.id) {
    notification.error({
      message: $t('deployer.page.target.error.nameRequired'),
    });
    return;
  }

  try {
    await targetStore.deleteTarget(row.id);
    notification.success({ message: $t('deployer.page.target.deleteSuccess') });
    await gridApi.query();
  } catch {
    notification.error({ message: $t('ui.notification.delete_failed') });
  }
}
</script>

<template>
  <Page auto-content-height>
    <Grid :table-title="$t('deployer.menu.target')">
      <template #toolbar-tools>
        <a-button class="mr-2" type="primary" @click="handleCreate">
          {{ $t('ui.button.create', { moduleName: '' }) }}
        </a-button>
      </template>
      <template #autoDeploy="{ row }">
        <a-tag :color="row.autoDeployOnRenewal ? '#52C41A' : '#8C8C8C'">
          {{ row.autoDeployOnRenewal ? $t('enum.enable.true') : $t('enum.enable.false') }}
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
          :title="$t('deployer.page.target.confirmDelete')"
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
