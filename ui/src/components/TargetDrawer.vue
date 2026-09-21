<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useTargets } from '@/stores/targets'
import { useConfigurations } from '@/stores/configurations'
import { describe } from '@/api/client'
import type { CertificateFilter, Target, TargetInput } from '@/api/types'

const props = defineProps<{ modelValue: boolean; target: Target | null }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; saved: []; removed: [] }>()

const store = useTargets()
const configs = useConfigurations()

const name = ref('')
const description = ref('')
const autoDeploy = ref(false)
const filters = ref<CertificateFilter[]>([])
const attached = ref<string[]>([])
const overridesText = ref('{}')
const error = ref('')
const busy = ref(false)

const editing = computed(() => props.target !== null)

watch(
  () => props.modelValue,
  (open) => {
    if (!open) return
    void configs.list()
    error.value = ''
    const t = props.target
    name.value = t?.name ?? ''
    description.value = t?.description ?? ''
    autoDeploy.value = t?.auto_deploy ?? false
    filters.value = (t?.certificate_filters ?? []).map((f) => ({ ...f }))
    attached.value = [...(t?.configuration_ids ?? [])]
    overridesText.value = '{}'
  },
)

function addFilter(): void {
  filters.value.push({})
}
function removeFilter(i: number): void {
  filters.value.splice(i, 1)
}

function build(): TargetInput {
  const clean = filters.value
    .map((f) => Object.fromEntries(Object.entries(f).filter(([, v]) => v)))
    .filter((f) => Object.keys(f).length) as CertificateFilter[]
  return {
    name: name.value.trim(),
    description: description.value.trim() || undefined,
    auto_deploy: autoDeploy.value,
    certificate_filters: clean,
  }
}

async function save(): Promise<void> {
  busy.value = true
  error.value = ''
  try {
    const input = build()
    let id: string
    if (editing.value && props.target) {
      await store.update(props.target.id, input)
      id = props.target.id
    } else {
      const t = await store.create(input)
      id = t.id
    }
    // Reconcile configuration attachments.
    const before = new Set(props.target?.configuration_ids ?? [])
    const now = new Set(attached.value)
    const toAttach = [...now].filter((c) => !before.has(c))
    const toDetach = [...before].filter((c) => !now.has(c))
    let overrides: Record<string, Record<string, unknown>> | undefined
    const parsed = JSON.parse(overridesText.value || '{}')
    if (parsed && Object.keys(parsed).length) overrides = parsed
    if (toAttach.length) await store.attach(id, toAttach, overrides)
    if (toDetach.length) await store.detach(id, toDetach)
    emit('saved')
    emit('update:modelValue', false)
  } catch (e) {
    error.value = describe(e) === 'Something went wrong.' ? (e as Error).message : describe(e)
  } finally {
    busy.value = false
  }
}

async function remove(): Promise<void> {
  if (!props.target) return
  busy.value = true
  try {
    await store.remove(props.target.id)
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
  <v-navigation-drawer :model-value="modelValue" location="right" temporary width="500" @update:model-value="emit('update:modelValue', $event)">
    <v-toolbar density="comfortable" :title="editing ? 'Edit target' : 'New target'">
      <v-btn icon="mdi-close" variant="text" @click="emit('update:modelValue', false)" />
    </v-toolbar>
    <div class="pa-4">
      <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-3">{{ error }}</v-alert>
      <v-text-field v-model="name" label="Name" density="comfortable" data-test="target-name" />
      <v-text-field v-model="description" label="Description" density="comfortable" />
      <v-switch v-model="autoDeploy" label="Auto-deploy on issue/renewal" color="primary" density="comfortable" data-test="target-auto" />

      <div class="d-flex align-center mt-2 mb-1">
        <span class="text-subtitle-2">Certificate filters</span>
        <v-spacer />
        <v-btn size="small" variant="text" prepend-icon="mdi-plus" @click="addFilter">Add</v-btn>
      </div>
      <p class="text-caption text-medium-emphasis mb-2">Empty filters match all certificates. Rules within a filter are AND-matched.</p>
      <v-card v-for="(f, i) in filters" :key="i" variant="outlined" class="mb-2 pa-3">
        <div class="d-flex justify-end">
          <v-btn size="x-small" icon="mdi-delete" variant="text" @click="removeFilter(i)" />
        </div>
        <v-text-field v-model="f.issuer" label="Issuer (exact)" density="compact" hide-details class="mb-2" />
        <v-text-field v-model="f.common_name" label="Common name (regex)" density="compact" hide-details class="mb-2" />
        <v-text-field v-model="f.san" label="SAN (regex)" density="compact" hide-details class="mb-2" />
        <v-text-field v-model="f.organization" label="Organization (exact)" density="compact" hide-details />
      </v-card>

      <v-select
        v-model="attached"
        :items="configs.items"
        item-title="name"
        item-value="id"
        label="Attached configurations"
        multiple
        chips
        density="comfortable"
        class="mt-3"
        data-test="target-configs"
      />
      <v-textarea
        v-model="overridesText"
        label="Per-config overrides (JSON: {configId: {...}})"
        rows="3"
        density="comfortable"
        auto-grow
        class="mono"
        hint="Config overlay only — never credentials. Applied to newly attached configs."
        persistent-hint
      />

      <div class="d-flex ga-2 mt-4">
        <v-btn color="primary" :loading="busy" data-test="target-save" @click="save">Save</v-btn>
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
