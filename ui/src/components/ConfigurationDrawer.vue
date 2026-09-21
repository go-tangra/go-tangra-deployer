<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useConfigurations } from '@/stores/configurations'
import { useProviders } from '@/stores/providers'
import { describe } from '@/api/client'
import type { Configuration, ConfigurationInput } from '@/api/types'

const props = defineProps<{ modelValue: boolean; configuration: Configuration | null }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; saved: []; removed: [] }>()

const store = useConfigurations()
const providers = useProviders()

const name = ref('')
const description = ref('')
const providerType = ref('')
const configText = ref('{}')
const credsText = ref('{}')
const error = ref('')
const validateMsg = ref('')
const busy = ref(false)

const editing = computed(() => props.configuration !== null)
const provider = computed(() => providers.get(providerType.value))

watch(
  () => props.modelValue,
  (open) => {
    if (!open) return
    void providers.list()
    error.value = ''
    validateMsg.value = ''
    const c = props.configuration
    name.value = c?.name ?? ''
    description.value = c?.description ?? ''
    providerType.value = c?.provider_type ?? ''
    configText.value = JSON.stringify(c?.config ?? {}, null, 2)
    credsText.value = '{}'
  },
)

function parse(text: string): Record<string, unknown> {
  const v = JSON.parse(text || '{}')
  if (typeof v !== 'object' || v === null || Array.isArray(v)) throw new Error('must be a JSON object')
  return v as Record<string, unknown>
}

function build(): ConfigurationInput {
  const input: ConfigurationInput = {
    name: name.value.trim(),
    description: description.value.trim() || undefined,
    provider_type: providerType.value,
    config: parse(configText.value),
  }
  const creds = parse(credsText.value)
  if (Object.keys(creds).length) input.credentials = creds
  return input
}

async function save(): Promise<void> {
  busy.value = true
  error.value = ''
  try {
    const input = build()
    if (editing.value && props.configuration) await store.update(props.configuration.id, input)
    else await store.create(input)
    emit('saved')
    emit('update:modelValue', false)
  } catch (e) {
    error.value = describe(e) === 'Something went wrong.' ? (e as Error).message : describe(e)
  } finally {
    busy.value = false
  }
}

async function validate(): Promise<void> {
  busy.value = true
  error.value = ''
  validateMsg.value = ''
  try {
    await store.validate(providerType.value, parse(credsText.value), parse(configText.value))
    validateMsg.value = 'Credentials are valid.'
  } catch (e) {
    error.value = describe(e)
  } finally {
    busy.value = false
  }
}

async function remove(): Promise<void> {
  if (!props.configuration) return
  busy.value = true
  try {
    await store.remove(props.configuration.id)
    emit('removed')
    emit('update:modelValue', false)
  } catch (e) {
    error.value = describe(e)
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <v-navigation-drawer :model-value="modelValue" location="right" temporary width="460" @update:model-value="emit('update:modelValue', $event)">
    <v-toolbar density="comfortable" :title="editing ? 'Edit configuration' : 'New configuration'">
      <v-btn icon="mdi-close" variant="text" @click="emit('update:modelValue', false)" />
    </v-toolbar>
    <div class="pa-4">
      <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-3">{{ error }}</v-alert>
      <v-alert v-if="validateMsg" type="success" variant="tonal" density="compact" class="mb-3">{{ validateMsg }}</v-alert>
      <v-text-field v-model="name" label="Name" density="comfortable" data-test="config-name" />
      <v-text-field v-model="description" label="Description" density="comfortable" />
      <v-select
        v-model="providerType"
        :items="providers.items"
        item-title="display_name"
        item-value="type"
        label="Provider"
        density="comfortable"
        :disabled="editing"
        data-test="config-provider"
      />
      <p v-if="provider?.required_config?.length" class="text-caption text-medium-emphasis mb-1">
        Config keys: {{ provider.required_config.join(', ') }}
      </p>
      <v-textarea v-model="configText" label="Config (JSON)" rows="4" density="comfortable" auto-grow class="mono" />
      <p v-if="provider?.required_credentials?.length" class="text-caption text-medium-emphasis mb-1">
        Credential keys: {{ provider.required_credentials.join(', ') }}
      </p>
      <v-textarea
        v-model="credsText"
        label="Credentials (JSON, write-only)"
        rows="4"
        density="comfortable"
        auto-grow
        class="mono"
        hint="Sealed at rest; never returned. Leave as {} to keep existing."
        persistent-hint
      />
      <div class="d-flex ga-2 mt-4">
        <v-btn color="primary" :loading="busy" data-test="config-save" @click="save">Save</v-btn>
        <v-btn variant="tonal" :loading="busy" @click="validate">Validate</v-btn>
        <v-spacer />
        <v-btn v-if="editing" color="error" variant="text" :loading="busy" @click="remove">Delete</v-btn>
      </div>
    </div>
  </v-navigation-drawer>
</template>

<style scoped>
.mono :deep(textarea) {
  font-family: monospace;
  font-size: 0.85rem;
}
</style>
