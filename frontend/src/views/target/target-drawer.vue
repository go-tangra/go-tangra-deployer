<script lang="ts" setup>
import { computed, ref } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';
import { LucidePlus, LucideTrash } from 'shell/vben/icons';

import {
  Alert,
  Button,
  Descriptions,
  DescriptionsItem,
  Divider,
  Form,
  FormItem,
  Input,
  Modal,
  notification,
  Select,
  Switch,
  Table,
  Tag,
  Textarea,
} from 'ant-design-vue';

import {
  type CertificateFilter,
  type DeploymentTarget,
  type TargetConfiguration,
} from '../../api/services';
import { $t } from 'shell/locales';
import { useDeployerConfigurationStore } from '../../stores/deployer-configuration.state';
import { useDeployerTargetStore } from '../../stores/deployer-target.state';
import { formatDateTime as formatDateTimeShared } from '../../datetime';
import { useTenantStore } from 'shell/stores';

const targetStore = useDeployerTargetStore();
const configStore = useDeployerConfigurationStore();
const tenantStore = useTenantStore();

const data =
  ref<{
    mode: 'create' | 'edit' | 'view';
    row: DeploymentTarget;
  }>();
const loading = ref(false);
const tenantOptions = ref<Array<{ label: string; value: number }>>([]);
const availableConfigurations = ref<TargetConfiguration[]>([]);
const linkedConfigurations = ref<TargetConfiguration[]>([]);
const selectedConfigIds = ref<string[]>([]);

// Form state
const formState = ref<{
  autoDeployOnRenewal: boolean;
  certificateFilters: CertificateFilter[];
  configurationIds: string[];
  description: string;
  name: string;
  tenantId: number | undefined;
}>({
  name: '',
  description: '',
  autoDeployOnRenewal: false,
  tenantId: undefined,
  certificateFilters: [],
  configurationIds: [],
});

// Certificate filter management
function addFilter() {
  formState.value.certificateFilters.push({
    issuerName: undefined,
    commonNamePattern: undefined,
    sanPattern: undefined,
    subjectOrganization: undefined,
    subjectOrgUnit: undefined,
    subjectCountry: undefined,
    labels: undefined,
  });
}

function removeFilter(index: number) {
  formState.value.certificateFilters.splice(index, 1);
}

const title = computed(() => {
  if (data.value?.mode === 'create') {
    return $t('deployer.page.target.create');
  }
  if (data.value?.mode === 'edit') {
    return $t('deployer.page.target.edit');
  }
  return $t('deployer.page.target.details');
});

function formatDateTime(value: string | undefined) {
  if (!value) return '-';
  try {
    return formatDateTimeShared(value);
  } catch {
    return value;
  }
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

async function loadAvailableConfigurations() {
  try {
    const result = await configStore.listConfigurations(
      { page: 1, pageSize: 100 },
      { tenantId: formState.value.tenantId },
    );
    availableConfigurations.value = result.items ?? [];
  } catch (e) {
    console.error('Failed to load configurations:', e);
  }
}

// Per-configuration provider config overrides, keyed by configuration id.
// Lets one shared configuration (a single Cloudflare API token) be pointed at a
// different zone_id per target instead of duplicating the configuration.
const configOverrides = ref<Record<string, Record<string, string>>>({});

const overrideModalOpen = ref(false);
const overrideConfigId = ref('');
const overrideConfigName = ref('');
const overrideRows = ref<{ key: string; value: string }[]>([]);

function overrideSummary(configId: string): string {
  const fields = configOverrides.value[configId];
  if (!fields) return '';
  const entries = Object.entries(fields).filter(([k]) => k);
  if (entries.length === 0) return '';
  return entries.map(([k, v]) => `${k}=${v}`).join(', ');
}

function openOverrideEditor(config: any) {
  overrideConfigId.value = config.id;
  overrideConfigName.value = config.name ?? config.id;
  const existing = configOverrides.value[config.id] ?? {};
  overrideRows.value = Object.entries(existing).map(([key, value]) => ({
    key,
    value: String(value),
  }));
  // Seed an empty row so the editor is usable immediately, and prefill the key
  // the provider requires when we can infer it.
  if (overrideRows.value.length === 0) {
    overrideRows.value = [
      { key: config.providerType === 'cloudflare' ? 'zone_id' : '', value: '' },
    ];
  }
  overrideModalOpen.value = true;
}

function addOverrideRow() {
  overrideRows.value.push({ key: '', value: '' });
}

function removeOverrideRow(index: number) {
  overrideRows.value.splice(index, 1);
}

async function saveOverrides() {
  if (!data.value?.row.id) return;

  const fields: Record<string, string> = {};
  for (const { key, value } of overrideRows.value) {
    const k = key.trim();
    if (k) fields[k] = value;
  }

  const next = { ...configOverrides.value };
  if (Object.keys(fields).length === 0) {
    // Clearing every field removes the override entirely rather than storing an
    // empty map, so the configuration's own config applies again.
    delete next[overrideConfigId.value];
  } else {
    next[overrideConfigId.value] = fields;
  }

  loading.value = true;
  try {
    // The whole map is replaced, so send the full set.
    await targetStore.updateTarget(data.value.row.id, {
      configOverrides: next,
    } as any);
    configOverrides.value = next;
    notification.success({ message: $t('deployer.page.target.updateSuccess') });
    overrideModalOpen.value = false;
  } catch (e) {
    console.error('Failed to save config overrides:', e);
    notification.error({ message: $t('ui.notification.save_failed') });
  } finally {
    loading.value = false;
  }
}

async function loadLinkedConfigurations(targetId: string) {
  try {
    const result = await targetStore.listTargetConfigurations(targetId, { page: 1, pageSize: 100 });
    linkedConfigurations.value = result.items ?? [];
    // The overrides live on the target, not on the configuration rows.
    const row: any = data.value?.row ?? {};
    configOverrides.value = { ...(row.configOverrides ?? {}) };
  } catch (e) {
    console.error('Failed to load linked configurations:', e);
  }
}

async function handleAddConfigurations() {
  if (!data.value?.row.id || selectedConfigIds.value.length === 0) return;

  loading.value = true;
  try {
    await targetStore.addConfigurations(data.value.row.id, selectedConfigIds.value);
    notification.success({ message: $t('deployer.page.target.updateSuccess') });
    await loadLinkedConfigurations(data.value.row.id);
    selectedConfigIds.value = [];
  } catch (e) {
    console.error('Failed to add configurations:', e);
    notification.error({ message: $t('ui.notification.save_failed') });
  } finally {
    loading.value = false;
  }
}

async function handleRemoveConfiguration(configId: string) {
  if (!data.value?.row.id) return;

  loading.value = true;
  try {
    await targetStore.removeConfigurations(data.value.row.id, [configId]);
    notification.success({ message: $t('deployer.page.target.updateSuccess') });
    await loadLinkedConfigurations(data.value.row.id);
  } catch (e) {
    console.error('Failed to remove configuration:', e);
    notification.error({ message: $t('ui.notification.save_failed') });
  } finally {
    loading.value = false;
  }
}

const configurationColumns = [
  { title: $t('deployer.page.configuration.name'), dataIndex: 'name', key: 'name' },
  { title: $t('deployer.page.configuration.providerType'), dataIndex: 'providerType', key: 'providerType' },
  { title: $t('deployer.page.target.configOverrides'), key: 'overrides' },
  { title: $t('ui.table.action'), key: 'action', width: 140 },
];

// Filter out already linked configurations
const unlinkedConfigurations = computed(() => {
  const linkedIds = new Set(linkedConfigurations.value.map((c) => c.id));
  return availableConfigurations.value.filter((c) => !linkedIds.has(c.id));
});

async function handleSubmit() {
  loading.value = true;
  try {
    // Clean up empty filters
    const cleanFilters = formState.value.certificateFilters.filter((f) => {
      return (
        f.issuerName ||
        f.commonNamePattern ||
        f.sanPattern ||
        f.subjectOrganization ||
        f.subjectOrgUnit ||
        f.subjectCountry ||
        (f.labels && f.labels.length > 0)
      );
    });

    if (data.value?.mode === 'create') {
      await targetStore.createTarget({
        tenantId: formState.value.tenantId ?? 0,
        name: formState.value.name,
        description: formState.value.description,
        autoDeployOnRenewal: formState.value.autoDeployOnRenewal,
        certificateFilters: cleanFilters as any,
        configurationIds: formState.value.configurationIds,
      });
      notification.success({
        message: $t('deployer.page.target.createSuccess'),
      });
    } else if (data.value?.mode === 'edit') {
      await targetStore.updateTarget(data.value.row.id!, {
        name: formState.value.name,
        description: formState.value.description,
        autoDeployOnRenewal: formState.value.autoDeployOnRenewal,
        certificateFilters: cleanFilters as any,
      });
      notification.success({
        message: $t('deployer.page.target.updateSuccess'),
      });
    }
    drawerApi.close();
  } catch (e) {
    console.error('Failed to save target:', e);
    notification.error({ message: $t('ui.notification.save_failed') });
  } finally {
    loading.value = false;
  }
}

function resetForm() {
  formState.value = {
    name: '',
    description: '',
    autoDeployOnRenewal: false,
    tenantId: undefined,
    certificateFilters: [],
    configurationIds: [],
  };
  linkedConfigurations.value = [];
  selectedConfigIds.value = [];
}

function populateFormFromRow(row: DeploymentTarget) {
  formState.value = {
    name: row.name ?? '',
    description: row.description ?? '',
    autoDeployOnRenewal: row.autoDeployOnRenewal ?? false,
    tenantId: row.tenantId,
    certificateFilters: row.certificateFilters ?? [],
    configurationIds: [],
  };
}

const [Drawer, drawerApi] = useVbenDrawer({
  onCancel() {
    drawerApi.close();
  },

  async onOpenChange(isOpen) {
    if (isOpen) {
      data.value = drawerApi.getData() as {
        mode: 'create' | 'edit' | 'view';
        row: DeploymentTarget;
      };
      if (data.value?.mode === 'create') {
        resetForm();
        await Promise.all([loadTenants(), loadAvailableConfigurations()]);
      } else if (data.value?.mode === 'edit') {
        populateFormFromRow(data.value.row);
        await Promise.all([
          loadAvailableConfigurations(),
          data.value.row.id ? loadLinkedConfigurations(data.value.row.id) : Promise.resolve(),
        ]);
      } else if (data.value?.mode === 'view' && data.value.row.id) {
        await loadLinkedConfigurations(data.value.row.id);
      }
    }
  },
});

const target = computed(() => data.value?.row);
const isViewMode = computed(() => data.value?.mode === 'view');
const isCreateMode = computed(() => data.value?.mode === 'create');
const isEditMode = computed(() => data.value?.mode === 'edit');
</script>

<template>
  <Drawer :title="title" :footer="false">
    <!-- View Mode -->
    <template v-if="target && isViewMode">
      <Descriptions :column="1" bordered size="small">
        <DescriptionsItem :label="$t('deployer.page.target.id')">
          {{ target.id }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.target.name')">
          {{ target.name }}
        </DescriptionsItem>
        <DescriptionsItem
          :label="$t('deployer.page.target.autoDeployOnRenewal')"
        >
          <Tag :color="target.autoDeployOnRenewal ? '#52C41A' : '#8C8C8C'">
            {{
              target.autoDeployOnRenewal
                ? $t('enum.enable.true')
                : $t('enum.enable.false')
            }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.description')">
          {{ target.description ?? '-' }}
        </DescriptionsItem>
      </Descriptions>

      <!-- Linked Configurations -->
      <Divider>{{ $t('deployer.page.target.configurations') }}</Divider>
      <div v-if="linkedConfigurations.length === 0" class="text-gray-500 text-center py-4">
        {{ $t('deployer.page.target.noConfigurations') }}
      </div>
      <Table
        v-else
        :columns="configurationColumns"
        :data-source="linkedConfigurations"
        :pagination="false"
        size="small"
        row-key="id"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'providerType'">
            <Tag color="blue">{{ record.providerType }}</Tag>
          </template>
          <template v-else-if="column.key === 'overrides'">
            <Tag v-if="overrideSummary(record.id)" color="purple">
              {{ overrideSummary(record.id) }}
            </Tag>
            <span v-else class="text-gray-500">—</span>
          </template>
        </template>
      </Table>

      <!-- Certificate Filters -->
      <template
        v-if="
          target.certificateFilters && target.certificateFilters.length > 0
        "
      >
        <Divider>{{ $t('deployer.page.target.certificateFilters') }}</Divider>
        <div
          v-for="(filter, index) in target.certificateFilters"
          :key="index"
          class="mb-3 p-3 border border-gray-200 rounded-lg"
        >
          <div class="text-sm font-medium mb-2">
            {{ $t('deployer.page.target.filter.title', { index: index + 1 }) }}
          </div>
          <Descriptions :column="1" bordered size="small">
            <DescriptionsItem
              v-if="filter.issuerName"
              :label="$t('deployer.page.target.filter.issuerName')"
            >
              {{ filter.issuerName }}
            </DescriptionsItem>
            <DescriptionsItem
              v-if="filter.commonNamePattern"
              :label="$t('deployer.page.target.filter.commonNamePattern')"
            >
              <code>{{ filter.commonNamePattern }}</code>
            </DescriptionsItem>
            <DescriptionsItem
              v-if="filter.sanPattern"
              :label="$t('deployer.page.target.filter.sanPattern')"
            >
              <code>{{ filter.sanPattern }}</code>
            </DescriptionsItem>
            <DescriptionsItem
              v-if="filter.subjectOrganization"
              :label="$t('deployer.page.target.filter.subjectOrganization')"
            >
              {{ filter.subjectOrganization }}
            </DescriptionsItem>
            <DescriptionsItem
              v-if="filter.subjectOrgUnit"
              :label="$t('deployer.page.target.filter.subjectOrgUnit')"
            >
              {{ filter.subjectOrgUnit }}
            </DescriptionsItem>
            <DescriptionsItem
              v-if="filter.subjectCountry"
              :label="$t('deployer.page.target.filter.subjectCountry')"
            >
              {{ filter.subjectCountry }}
            </DescriptionsItem>
          </Descriptions>
        </div>
      </template>

      <Divider>{{ $t('ui.table.timestamps') }}</Divider>
      <Descriptions :column="2" bordered size="small">
        <DescriptionsItem :label="$t('ui.table.createdAt')">
          {{ formatDateTime(target.createTime) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.updatedAt')">
          {{ formatDateTime(target.updateTime) }}
        </DescriptionsItem>
      </Descriptions>
    </template>

    <!-- Create/Edit Mode -->
    <template v-else-if="isCreateMode || isEditMode">
      <Form layout="vertical" :model="formState" @finish="handleSubmit">
        <Divider orientation="left">{{ $t('deployer.page.configuration.basicInfo') }}</Divider>

        <FormItem
          v-if="isCreateMode"
          :label="$t('deployer.page.target.tenant')"
          name="tenantId"
        >
          <Select
            v-model:value="formState.tenantId"
            :options="tenantOptions"
            :placeholder="$t('ui.placeholder.select')"
            allowClear
            @change="loadAvailableConfigurations"
          />
        </FormItem>

        <FormItem
          :label="$t('deployer.page.target.name')"
          name="name"
          :rules="[{ required: true, message: 'Name is required' }]"
        >
          <Input
            v-model:value="formState.name"
            placeholder="my-deployment-group"
            :maxlength="128"
          />
        </FormItem>

        <FormItem
          :label="$t('deployer.page.target.autoDeployOnRenewal')"
          name="autoDeployOnRenewal"
        >
          <Switch v-model:checked="formState.autoDeployOnRenewal" />
          <span class="ml-2 text-gray-500">{{
            $t('deployer.page.target.autoDeployOnRenewalHint')
          }}</span>
        </FormItem>

        <FormItem :label="$t('ui.table.description')" name="description">
          <Textarea
            v-model:value="formState.description"
            :rows="2"
            :maxlength="512"
          />
        </FormItem>

        <!-- Configurations selection (create mode only) -->
        <template v-if="isCreateMode">
          <Divider orientation="left">{{ $t('deployer.page.target.configurations') }}</Divider>
          <FormItem :label="$t('deployer.page.target.selectConfigurations')">
            <Select
              v-model:value="formState.configurationIds"
              mode="multiple"
              :placeholder="$t('ui.placeholder.select')"
              style="width: 100%"
            >
              <Select.Option
                v-for="config in availableConfigurations"
                :key="config.id"
                :value="config.id"
              >
                {{ config.name }} ({{ config.providerType }})
              </Select.Option>
            </Select>
          </FormItem>
        </template>

        <!-- Linked Configurations management (edit mode) -->
        <template v-if="isEditMode">
          <Divider orientation="left">{{ $t('deployer.page.target.configurations') }}</Divider>

          <!-- Add configurations -->
          <div class="flex gap-2 mb-4">
            <Select
              v-model:value="selectedConfigIds"
              mode="multiple"
              :placeholder="$t('deployer.page.target.selectConfigurations')"
              style="flex: 1"
            >
              <Select.Option
                v-for="config in unlinkedConfigurations"
                :key="config.id"
                :value="config.id"
              >
                {{ config.name }} ({{ config.providerType }})
              </Select.Option>
            </Select>
            <Button
              type="primary"
              :disabled="selectedConfigIds.length === 0"
              :loading="loading"
              @click="handleAddConfigurations"
            >
              <LucidePlus class="size-4" />
            </Button>
          </div>

          <!-- Linked configurations list -->
          <Table
            :columns="configurationColumns"
            :data-source="linkedConfigurations"
            :pagination="false"
            size="small"
            row-key="id"
          >
            <template #bodyCell="{ column, record }">
              <template v-if="column.key === 'providerType'">
                <Tag color="blue">{{ record.providerType }}</Tag>
              </template>
              <template v-else-if="column.key === 'overrides'">
                <Tag v-if="overrideSummary(record.id)" color="purple">
                  {{ overrideSummary(record.id) }}
                </Tag>
                <span v-else class="text-gray-500">—</span>
              </template>
              <template v-else-if="column.key === 'action'">
                <Button type="text" size="small" @click="openOverrideEditor(record)">
                  {{ $t('deployer.page.target.override') }}
                </Button>
                <a-popconfirm
                  :title="$t('deployer.page.target.removeConfiguration') + '?'"
                  @confirm="handleRemoveConfiguration(record.id)"
                >
                  <Button type="text" danger size="small">
                    <LucideTrash class="size-4" />
                  </Button>
                </a-popconfirm>
              </template>
            </template>
          </Table>
        </template>

        <!-- Certificate Filters (only show when auto-deploy is enabled) -->
        <template v-if="formState.autoDeployOnRenewal">
          <Divider orientation="left">{{
            $t('deployer.page.target.certificateFilters')
          }}</Divider>
          <div class="mb-2 text-gray-500 text-sm">
            {{ $t('deployer.page.target.certificateFiltersHint') }}
          </div>

          <div
            v-for="(filter, index) in formState.certificateFilters"
            :key="index"
            class="mb-4 p-3 border border-gray-200 rounded-lg relative"
          >
            <Button
              type="text"
              danger
              size="small"
              class="absolute top-2 right-2"
              @click="removeFilter(index)"
            >
              {{ $t('ui.button.remove') }}
            </Button>

            <div class="text-sm font-medium mb-2">
              {{ $t('deployer.page.target.filter.title', { index: index + 1 }) }}
            </div>

            <FormItem
              :label="$t('deployer.page.target.filter.issuerName')"
              class="mb-2"
            >
              <Input
                v-model:value="filter.issuerName"
                :placeholder="$t('deployer.page.target.filter.issuerNamePlaceholder')"
              />
            </FormItem>

            <FormItem
              :label="$t('deployer.page.target.filter.commonNamePattern')"
              class="mb-2"
            >
              <Input
                v-model:value="filter.commonNamePattern"
                :placeholder="$t('deployer.page.target.filter.commonNamePatternPlaceholder')"
              />
            </FormItem>

            <FormItem
              :label="$t('deployer.page.target.filter.sanPattern')"
              class="mb-2"
            >
              <Input
                v-model:value="filter.sanPattern"
                :placeholder="$t('deployer.page.target.filter.sanPatternPlaceholder')"
              />
            </FormItem>

            <FormItem
              :label="$t('deployer.page.target.filter.subjectOrganization')"
              class="mb-2"
            >
              <Input
                v-model:value="filter.subjectOrganization"
                :placeholder="$t('deployer.page.target.filter.subjectOrganizationPlaceholder')"
              />
            </FormItem>

            <FormItem
              :label="$t('deployer.page.target.filter.subjectOrgUnit')"
              class="mb-2"
            >
              <Input
                v-model:value="filter.subjectOrgUnit"
                :placeholder="$t('deployer.page.target.filter.subjectOrgUnitPlaceholder')"
              />
            </FormItem>

            <FormItem
              :label="$t('deployer.page.target.filter.subjectCountry')"
              class="mb-2"
            >
              <Input
                v-model:value="filter.subjectCountry"
                :placeholder="$t('deployer.page.target.filter.subjectCountryPlaceholder')"
                :maxlength="2"
              />
            </FormItem>
          </div>

          <Button type="dashed" block @click="addFilter">
            + {{ $t('deployer.page.target.addFilter') }}
          </Button>
        </template>

        <Divider />

        <FormItem>
          <Button type="primary" html-type="submit" :loading="loading" block>
            {{
              isCreateMode
                ? $t('ui.button.create', { moduleName: '' })
                : $t('ui.button.save')
            }}
          </Button>
        </FormItem>
      </Form>
    </template>
  </Drawer>
  <!-- Per-configuration provider config overrides -->
  <Modal
    v-model:open="overrideModalOpen"
    :title="$t('deployer.page.target.overrideTitle', { name: overrideConfigName })"
    :confirm-loading="loading"
    @ok="saveOverrides"
  >
    <Alert
      type="info"
      show-icon
      class="mb-3"
      :message="$t('deployer.page.target.overrideHint')"
    />
    <div
      v-for="(row, index) in overrideRows"
      :key="index"
      class="flex gap-2 mb-2 items-center"
    >
      <Input
        v-model:value="row.key"
        :placeholder="$t('deployer.page.target.overrideField')"
        style="flex: 1"
      />
      <Input
        v-model:value="row.value"
        :placeholder="$t('deployer.page.target.overrideValue')"
        style="flex: 2"
      />
      <Button type="text" danger size="small" @click="removeOverrideRow(index)">
        <LucideTrash class="size-4" />
      </Button>
    </div>
    <Button type="dashed" block @click="addOverrideRow">
      {{ $t('deployer.page.target.overrideAddField') }}
    </Button>
  </Modal>
</template>
