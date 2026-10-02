<script setup lang="ts">
// Inventory host picker (descriptor type host_selector, feature 033 US6): a
// server-paged, searchable table of the inventory hosts the signed-in user may
// read, with agent online state and certificate capability, and the selection
// as chips. Without inventory access (401/403/404) it falls back to manual
// entry of host ids. v-model is the list of host ids.
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { UiAlert, UiBadge, UiDataTable, UiIcon, UiInput, UiTextarea, type Column } from '@go-tangra/ui'
import { agentsByHost, CAPABILITY, getHost, listHosts, unavailable, type InventoryAgent, type InventoryHost } from '@/api/inventory'
import { describe } from '@/api/client'

const props = defineProps<{ modelValue?: unknown | undefined; id: string; label: string; hint?: string | undefined; error?: string | undefined; required?: boolean | undefined; max?: number | undefined; disabled?: boolean | undefined }>()
const emit = defineEmits<{ (e: 'update:modelValue', v: string[]): void; (e: 'blur'): void }>()

const PAGE_SIZE = 10
const selected = computed<string[]>(() => (Array.isArray(props.modelValue) ? props.modelValue.map(String) : []))
const manual = ref(false)
const loadError = ref('')
const loading = ref(false)
const hosts = ref<InventoryHost[]>([])
const total = ref(0)
const page = ref(1)
const search = ref('')
const agents = ref<Map<string, InventoryAgent> | null>(null)
/** Hostnames of selected hosts (chips), filled from loaded pages and single reads. */
const names = ref<Record<string, string>>({})

type Row = InventoryHost & Record<string, unknown>
const rows = computed(() => hosts.value as Row[])

let seq = 0
async function load(): Promise<void> {
  const mine = ++seq
  loading.value = true
  loadError.value = ''
  try {
    const res = await listHosts({ page: page.value, page_size: PAGE_SIZE, hostname: search.value.trim() || undefined })
    if (mine !== seq) return
    hosts.value = res.items ?? []
    total.value = res.total ?? 0
    if (res.page && res.page !== page.value) page.value = res.page
    for (const h of hosts.value) names.value[h.id] = h.hostname
  } catch (e) {
    if (mine !== seq) return
    if (unavailable(e)) manual.value = true
    else loadError.value = describe(e)
  } finally {
    if (mine === seq) loading.value = false
  }
}

async function loadAgents(): Promise<void> {
  try {
    agents.value = await agentsByHost()
  } catch {
    agents.value = null // agent state needs agents:manage; the picker works without it
  }
}

/** Chip names for hosts selected before (edit): a few single reads. */
async function loadNames(): Promise<void> {
  const unknown = selected.value.filter((id) => !names.value[id]).slice(0, 20)
  await Promise.all(unknown.map(async (id) => {
    try {
      names.value[id] = (await getHost(id)).hostname
    } catch {
      /* unknown or foreign: the chip shows the id */
    }
  }))
}

let timer: ReturnType<typeof setTimeout> | null = null
watch(search, () => {
  if (timer) clearTimeout(timer)
  timer = setTimeout(() => {
    page.value = 1
    void load()
  }, 250)
})
onUnmounted(() => timer && clearTimeout(timer))
onMounted(async () => {
  await load()
  if (manual.value) return
  void loadAgents()
  void loadNames()
})
function setPage(p: number): void {
  page.value = p
  void load()
}

function toggle(id: string, on: boolean): void {
  if (on && props.max && selected.value.length >= props.max) return
  emit('update:modelValue', on ? [...new Set([...selected.value, id])] : selected.value.filter((x) => x !== id))
}
const isSelected = (id: string) => selected.value.includes(id)

// Manual entry: ids separated by new lines, commas or spaces.
const manualText = ref(selected.value.join('\n'))
function onManual(v: unknown): void {
  manualText.value = String(v ?? '')
  emit('update:modelValue', [...new Set(manualText.value.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean))])
}

const agentOf = (h: InventoryHost) => agents.value?.get(h.id)
const tagText = (t?: Record<string, string>) => Object.entries(t ?? {}).map(([k, v]) => (v ? `${k}=${v}` : k))
const columns: Column<Row>[] = [
  { key: 'select', label: 'Select' },
  { key: 'hostname', label: 'Host' },
  { key: 'tags', label: 'Tags', hideOnStack: true },
  { key: 'agent', label: 'Agent' },
]
const osOf = (h: InventoryHost) => [h.os_name, h.os_version].filter(Boolean).join(' ')
</script>

<template>
  <div class="flex flex-col gap-2" data-test="host-picker">
    <template v-if="manual">
      <UiAlert kind="info" data-test="host-picker-manual">Host list unavailable — enter host ids manually.</UiAlert>
      <UiTextarea :id="id" :label="label" :required="required" :error="props.error" :hint="hint ?? 'One host id per line.'" :rows="3" :model-value="manualText" :disabled="disabled" placeholder="0192a7c0-0000-7000-8000-000000000001" @update:model-value="onManual" @blur="emit('blur')" />
    </template>
    <template v-else>
      <UiInput :id="id" v-model="search" type="search" :label="label" :required="required" :error="props.error" :hint="hint" placeholder="Search by hostname, tick to select" autocomplete="off" :disabled="disabled" @blur="emit('blur')" />
      <ul v-if="selected.length" class="flex flex-wrap gap-1" aria-label="Selected hosts" data-test="host-chips">
        <li v-for="h in selected" :key="h" class="badge badge-soft badge-primary max-w-full gap-1">
          <span class="truncate">{{ names[h] ?? h }}</span>
          <button type="button" class="ms-0.5" :aria-label="'Remove ' + (names[h] ?? h)" :disabled="disabled" @click="toggle(h, false)"><UiIcon name="mdi-close" size="xs" /></button>
        </li>
      </ul>
      <UiAlert v-if="loadError" kind="error">{{ loadError }}</UiAlert>
      <div class="rounded-box border border-base-300">
        <UiDataTable :items="rows" :columns="columns" :loading="loading" :total="total" :page="page" :page-size="PAGE_SIZE" :page-sizes="[PAGE_SIZE]" caption="Inventory hosts" empty-title="No hosts found" :row-attrs="(h) => ({ 'data-test': 'host-row-' + h.id })" data-test="host-table" @update:page="setPage">
          <template #cell-select="{ row }">
            <input type="checkbox" class="checkbox checkbox-sm checkbox-primary" :checked="isSelected(row.id)" :disabled="disabled || (!isSelected(row.id) && !!max && selected.length >= max)" :aria-label="'Select ' + row.hostname" @change="toggle(row.id, ($event.target as HTMLInputElement).checked)">
          </template>
          <template #cell-hostname="{ row }">
            <span class="flex flex-col"><span class="font-medium">{{ row.hostname }}</span><span v-if="osOf(row)" class="text-xs text-base-content/70">{{ osOf(row) }}</span></span>
          </template>
          <template #cell-tags="{ row }">
            <span class="flex flex-wrap gap-1"><UiBadge v-for="t in tagText(row.tags)" :key="t" size="xs">{{ t }}</UiBadge></span>
          </template>
          <template #cell-agent="{ row }">
            <span v-if="!agents" class="text-base-content/60">—</span>
            <span v-else class="flex flex-col items-start gap-1">
              <UiBadge v-if="!agentOf(row)" size="xs">no agent</UiBadge>
              <UiBadge v-else :color="agentOf(row)!.online ? 'success' : 'neutral'" size="xs">{{ agentOf(row)!.online ? 'online' : 'offline' }}</UiBadge>
              <UiBadge v-if="agentOf(row)?.certificate_capability" :color="CAPABILITY[agentOf(row)!.certificate_capability!]?.color ?? 'neutral'" size="xs" data-test="host-capability">{{ CAPABILITY[agentOf(row)!.certificate_capability!]?.text ?? agentOf(row)!.certificate_capability }}</UiBadge>
            </span>
          </template>
        </UiDataTable>
      </div>
    </template>
  </div>
</template>
