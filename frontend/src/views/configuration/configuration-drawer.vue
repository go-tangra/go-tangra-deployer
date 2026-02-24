<script lang="ts" setup>
import { computed, ref } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';

import {
  Button,
  Descriptions,
  DescriptionsItem,
  Divider,
  Form,
  FormItem,
  Input,
  notification,
  Select,
  Tag,
  Textarea,
} from 'ant-design-vue';

import {
  type ProviderInfo,
  type TargetConfiguration,
} from '../../api/services';
import { $t } from 'shell/locales';
import { useDeployerConfigurationStore } from '../../stores/deployer-configuration.state';
import { useTenantStore } from 'shell/stores';

const configStore = useDeployerConfigurationStore();
const tenantStore = useTenantStore();

const data =
  ref<{
    mode: 'create' | 'edit' | 'view';
    row: TargetConfiguration;
  }>();
const loading = ref(false);
const providers = ref<ProviderInfo[]>([]);
const tenantOptions = ref<Array<{ label: string; value: number }>>([]);

// Form state
const formState = ref<{
  config: Record<string, any>;
  credentials: Record<string, any>;
  description: string;
  name: string;
  providerType: string;
  status: string;
  tenantId: number | undefined;
}>({
  name: '',
  description: '',
  providerType: '',
  status: 'CONFIG_STATUS_ACTIVE',
  tenantId: undefined,
  credentials: {},
  config: {},
});

const title = computed(() => {
  if (data.value?.mode === 'create') {
    return $t('deployer.page.configuration.create');
  }
  if (data.value?.mode === 'edit') {
    return $t('deployer.page.configuration.edit');
  }
  return $t('deployer.page.configuration.details');
});

function statusToColor(status: string | undefined) {
  switch (status) {
    case 'CONFIG_STATUS_ACTIVE':
      return '#52C41A';
    case 'CONFIG_STATUS_INACTIVE':
      return '#8C8C8C';
    case 'CONFIG_STATUS_ERROR':
      return '#FF4D4F';
    default:
      return '#C9CDD4';
  }
}

function statusToName(status: string | undefined) {
  switch (status) {
    case 'CONFIG_STATUS_ACTIVE':
      return $t('deployer.enum.configStatus.active');
    case 'CONFIG_STATUS_INACTIVE':
      return $t('deployer.enum.configStatus.inactive');
    case 'CONFIG_STATUS_ERROR':
      return $t('deployer.enum.configStatus.error');
    default:
      return status ?? '';
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

async function loadProviders() {
  try {
    const resp = await configStore.listProviders();
    providers.value = (resp.providers ?? []) as ProviderInfo[];
  } catch (e) {
    console.error('Failed to load providers:', e);
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

const selectedProviderInfo = computed(() => {
  return providers.value.find((p) => p.type === formState.value.providerType);
});

// Optional config fields by provider type
const optionalConfigFields: Record<string, string[]> = {
  bigip: ['ssl_profile'],
  webhook: ['verify_url', 'rollback_url', 'skip_tls_verify', 'timeout_seconds'],
};

// Optional credential fields by provider type
const optionalCredentialFields: Record<string, string[]> = {
  webhook: ['authorization', 'api_key', 'secret', 'token'],
};

const optionalConfigFieldsForProvider = computed(() => {
  return optionalConfigFields[formState.value.providerType] ?? [];
});

const optionalCredentialFieldsForProvider = computed(() => {
  return optionalCredentialFields[formState.value.providerType] ?? [];
});

// Get translated field label for a provider field
function getFieldLabel(field: string): string {
  const providerType = formState.value.providerType;
  const key = `deployer.providers.${providerType}.fields.${field}`;
  const translated = $t(key);
  return translated !== key ? translated : field;
}

// Get translated field placeholder
function getFieldPlaceholder(field: string): string {
  const providerType = formState.value.providerType;
  const key = `deployer.providers.${providerType}.fields.${field}_placeholder`;
  const translated = $t(key);
  return translated !== key ? translated : '';
}

// Get translated config field label for view mode
function getConfigFieldLabel(providerType: string | undefined, field: string): string {
  if (!providerType) return field;
  const key = `deployer.providers.${providerType}.fields.${field}`;
  const translated = $t(key);
  return translated !== key ? translated : field;
}

async function handleSubmit() {
  loading.value = true;
  try {
    if (data.value?.mode === 'create') {
      await configStore.createConfiguration({
        tenantId: formState.value.tenantId ?? 0,
        name: formState.value.name,
        description: formState.value.description,
        providerType: formState.value.providerType,
        credentials: formState.value.credentials as Record<string, never>,
        config: formState.value.config as Record<string, never>,
      });
      notification.success({
        message: $t('deployer.page.configuration.createSuccess'),
      });
    } else if (data.value?.mode === 'edit') {
      await configStore.updateConfiguration(data.value.row.id!, {
        name: formState.value.name,
        description: formState.value.description,
        credentials: formState.value.credentials as Record<string, never>,
        config: formState.value.config as Record<string, never>,
        status: formState.value.status as any,
      });
      notification.success({
        message: $t('deployer.page.configuration.updateSuccess'),
      });
    }
    drawerApi.close();
  } catch (e) {
    console.error('Failed to save configuration:', e);
    notification.error({ message: $t('ui.notification.save_failed') });
  } finally {
    loading.value = false;
  }
}

function resetForm() {
  formState.value = {
    name: '',
    description: '',
    providerType: '',
    status: 'CONFIG_STATUS_ACTIVE',
    tenantId: undefined,
    credentials: {},
    config: {},
  };
}

function populateFormFromRow(row: TargetConfiguration) {
  formState.value = {
    name: row.name ?? '',
    description: row.description ?? '',
    providerType: row.providerType ?? '',
    status: row.status ?? 'CONFIG_STATUS_ACTIVE',
    tenantId: row.tenantId,
    credentials: {},
    config: row.config ?? {},
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
        row: TargetConfiguration;
      };
      if (data.value?.mode === 'create') {
        resetForm();
        await Promise.all([loadProviders(), loadTenants()]);
      } else if (data.value?.mode === 'edit') {
        populateFormFromRow(data.value.row);
        await loadProviders();
      }
    }
  },
});

const configuration = computed(() => data.value?.row);
const isViewMode = computed(() => data.value?.mode === 'view');
const isCreateMode = computed(() => data.value?.mode === 'create');
const isEditMode = computed(() => data.value?.mode === 'edit');
</script>

<template>
  <Drawer :title="title" :footer="false">
    <!-- View Mode -->
    <template v-if="configuration && isViewMode">
      <Descriptions :column="1" bordered size="small">
        <DescriptionsItem :label="$t('deployer.page.configuration.id')">
          {{ configuration.id }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.configuration.name')">
          {{ configuration.name }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.configuration.providerType')">
          <Tag color="blue">
            {{ configuration.providerType }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.status')">
          <Tag :color="statusToColor(configuration.status)">
            {{ statusToName(configuration.status) }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('deployer.page.configuration.statusMessage')">
          {{ configuration.statusMessage ?? '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.description')">
          {{ configuration.description ?? '-' }}
        </DescriptionsItem>
      </Descriptions>

      <template v-if="configuration.config && Object.keys(configuration.config).length > 0">
        <Divider>{{ $t('deployer.page.configuration.config') }}</Divider>
        <Descriptions :column="1" bordered size="small">
          <DescriptionsItem
            v-for="(value, key) in configuration.config"
            :key="key"
            :label="getConfigFieldLabel(configuration.providerType, String(key))"
          >
            {{ value }}
          </DescriptionsItem>
        </Descriptions>
      </template>

      <Divider>{{ $t('ui.table.timestamps') }}</Divider>
      <Descriptions :column="2" bordered size="small">
        <DescriptionsItem :label="$t('deployer.page.configuration.lastDeploymentAt')">
          {{ formatDateTime(configuration.lastDeploymentAt) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.createdAt')">
          {{ formatDateTime(configuration.createTime) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('ui.table.updatedAt')">
          {{ formatDateTime(configuration.updateTime) }}
        </DescriptionsItem>
      </Descriptions>
    </template>

    <!-- Create/Edit Mode -->
    <template v-else-if="isCreateMode || isEditMode">
      <Form layout="vertical" :model="formState" @finish="handleSubmit">
        <Divider orientation="left">{{ $t('deployer.page.configuration.basicInfo') }}</Divider>

        <FormItem
          v-if="isCreateMode"
          :label="$t('deployer.page.configuration.tenant')"
          name="tenantId"
        >
          <Select
            v-model:value="formState.tenantId"
            :options="tenantOptions"
            :placeholder="$t('ui.placeholder.select')"
            allowClear
          />
        </FormItem>

        <FormItem
          :label="$t('deployer.page.configuration.name')"
          name="name"
          :rules="[{ required: true, message: 'Name is required' }]"
        >
          <Input
            v-model:value="formState.name"
            placeholder="my-cloudflare-config"
            :maxlength="128"
          />
        </FormItem>

        <FormItem
          v-if="isCreateMode"
          :label="$t('deployer.page.configuration.providerType')"
          name="providerType"
          :rules="[{ required: true, message: 'Provider type is required' }]"
        >
          <Select
            v-model:value="formState.providerType"
            :placeholder="$t('ui.placeholder.select')"
          >
            <Select.Option
              v-for="provider in providers"
              :key="provider.type"
              :value="provider.type"
            >
              {{ provider.displayName }} - {{ provider.description }}
            </Select.Option>
          </Select>
        </FormItem>

        <FormItem
          v-if="isEditMode"
          :label="$t('ui.table.status')"
          name="status"
        >
          <Select v-model:value="formState.status">
            <Select.Option value="CONFIG_STATUS_ACTIVE">{{
              $t('deployer.enum.configStatus.active')
            }}</Select.Option>
            <Select.Option value="CONFIG_STATUS_INACTIVE">{{
              $t('deployer.enum.configStatus.inactive')
            }}</Select.Option>
          </Select>
        </FormItem>

        <FormItem :label="$t('ui.table.description')" name="description">
          <Textarea
            v-model:value="formState.description"
            :rows="2"
            :maxlength="512"
          />
        </FormItem>

        <!-- Credentials -->
        <template
          v-if="
            selectedProviderInfo &&
            ((selectedProviderInfo.requiredCredentialFields &&
              selectedProviderInfo.requiredCredentialFields.length > 0) ||
              optionalCredentialFieldsForProvider.length > 0)
          "
        >
          <Divider orientation="left">{{
            $t('deployer.page.configuration.credentials')
          }}</Divider>
          <!-- Required credential fields -->
          <FormItem
            v-for="field in selectedProviderInfo.requiredCredentialFields"
            :key="field"
            :label="getFieldLabel(field)"
            :rules="[{ required: true }]"
          >
            <Input.Password
              :value="formState.credentials[field]"
              :placeholder="getFieldPlaceholder(field)"
              @update:value="(v: string) => (formState.credentials[field] = v)"
            />
          </FormItem>
          <!-- Optional credential fields -->
          <FormItem
            v-for="field in optionalCredentialFieldsForProvider"
            :key="field"
            :label="getFieldLabel(field)"
          >
            <Input.Password
              :value="formState.credentials[field]"
              :placeholder="getFieldPlaceholder(field)"
              @update:value="(v: string) => (formState.credentials[field] = v)"
            />
          </FormItem>
        </template>

        <!-- Config -->
        <template
          v-if="
            selectedProviderInfo &&
            ((selectedProviderInfo.requiredConfigFields &&
              selectedProviderInfo.requiredConfigFields.length > 0) ||
              optionalConfigFieldsForProvider.length > 0)
          "
        >
          <Divider orientation="left">{{
            $t('deployer.page.configuration.config')
          }}</Divider>
          <!-- Required config fields -->
          <FormItem
            v-for="field in selectedProviderInfo.requiredConfigFields"
            :key="field"
            :label="getFieldLabel(field)"
            :rules="[{ required: true }]"
          >
            <Input
              :value="formState.config[field]"
              :placeholder="getFieldPlaceholder(field)"
              @update:value="(v: string) => (formState.config[field] = v)"
            />
          </FormItem>
          <!-- Optional config fields -->
          <FormItem
            v-for="field in optionalConfigFieldsForProvider"
            :key="field"
            :label="getFieldLabel(field)"
          >
            <Input
              :value="formState.config[field]"
              :placeholder="getFieldPlaceholder(field)"
              @update:value="(v: string) => (formState.config[field] = v)"
            />
          </FormItem>
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
</template>
