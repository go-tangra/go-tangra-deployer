<script setup lang="ts">
// lcm certificate picker for the Deploy dialog: the operator types a host
// name and picks one of lcm's live certificates for it (exact names first,
// then the wildcard covering it, then names containing it). An exact match,
// or the only match, is preselected. v-model is the certificate id.
import { computed, onUnmounted, ref, watch } from 'vue'
import { UiAlert, UiBadge, UiInput } from '@go-tangra/ui'
import { ApiError, describe } from '@/api/client'
import { useConfigurations } from '@/stores/configurations'
import type { CertificateMatch } from '@/api/types'

const props = defineProps<{ modelValue?: unknown | undefined; id: string; error?: string | undefined; disabled?: boolean | undefined }>()
const emit = defineEmits<{ (e: 'update:modelValue', v: string): void; (e: 'blur'): void }>()

const store = useConfigurations()
const name = ref('')
const matches = ref<CertificateMatch[]>([])
const searched = ref('')
const loading = ref(false)
const loadError = ref('')
const selected = computed(() => (typeof props.modelValue === 'string' ? props.modelValue : ''))

let seq = 0
async function search(q: string): Promise<void> {
  const mine = ++seq
  loading.value = true
  loadError.value = ''
  try {
    const items = await store.findCertificates(q)
    if (mine !== seq) return
    matches.value = items
    searched.value = q
    const lower = q.toLowerCase().replace(/\.$/, '')
    const exact = items.find((m) => [m.subject, ...m.sans].some((n) => n.toLowerCase() === lower))
    const pick = exact ?? (items.length === 1 ? items[0] : undefined)
    if (!items.some((m) => m.id === selected.value)) emit('update:modelValue', pick?.id ?? '')
  } catch (e) {
    if (mine !== seq) return
    matches.value = []
    searched.value = q
    emit('update:modelValue', '')
    loadError.value = e instanceof ApiError && e.status === 422 ? 'Enter a host name: letters, digits, dots and hyphens.' : describe(e)
  } finally {
    if (mine === seq) loading.value = false
  }
}

let timer: ReturnType<typeof setTimeout> | null = null
watch(name, (v) => {
  if (timer) clearTimeout(timer)
  const q = v.trim()
  if (!q) {
    ++seq
    matches.value = []
    searched.value = ''
    loadError.value = ''
    loading.value = false
    emit('update:modelValue', '')
    return
  }
  timer = setTimeout(() => void search(q), 250)
})
onUnmounted(() => timer && clearTimeout(timer))

const others = (m: CertificateMatch) => m.sans.filter((s) => s.toLowerCase() !== m.subject.toLowerCase())
const expires = (m: CertificateMatch) => new Date(m.not_after).toLocaleDateString()
</script>

<template>
  <div class="flex flex-col gap-2" data-test="certificate-picker">
    <UiInput :id="id" v-model="name" type="search" label="Host name" hint="The certificate's host name, or part of it. A wildcard certificate covering it is listed too." placeholder="pbx.example.com" required autocomplete="off" :error="props.error" :disabled="disabled" data-test="deploy-hostname" @blur="emit('blur')" />
    <p v-if="loading" class="text-sm text-base-content/70" role="status">Searching lcm…</p>
    <UiAlert v-else-if="loadError" kind="error" data-test="certificate-error">{{ loadError }}</UiAlert>
    <UiAlert v-else-if="searched && !matches.length" kind="warning" data-test="certificate-none">No live certificate in lcm matches “{{ searched }}”.</UiAlert>
    <fieldset v-if="matches.length" class="flex flex-col gap-1" :disabled="disabled">
      <legend class="mb-1 text-sm font-medium">Certificate</legend>
      <label v-for="m in matches" :key="m.id" class="flex cursor-pointer items-start gap-2 rounded-box border border-base-300 p-2 has-[:checked]:border-primary" :data-test="'certificate-option-' + m.id">
        <input type="radio" class="radio radio-sm radio-primary mt-0.5" :name="id + '-certificate'" :value="m.id" :checked="m.id === selected" @change="emit('update:modelValue', m.id)">
        <span class="flex min-w-0 flex-col gap-0.5">
          <span class="flex flex-wrap items-center gap-1">
            <span class="font-medium break-all">{{ m.subject }}</span>
            <UiBadge v-if="m.status === 'expiring'" color="warning" size="xs">expiring</UiBadge>
            <UiBadge v-if="!m.has_key" color="warning" size="xs" data-test="certificate-no-key">no stored key</UiBadge>
          </span>
          <span v-if="others(m).length" class="text-xs break-all text-base-content/70">{{ others(m).join(', ') }}</span>
          <span class="text-xs text-base-content/70">Expires {{ expires(m) }} · serial {{ m.serial }}</span>
        </span>
      </label>
      <p v-if="matches.some((m) => !m.has_key)" class="text-xs text-base-content/70">“No stored key”: lcm does not hold this certificate's private key, so providers that install the key cannot deploy it.</p>
    </fieldset>
  </div>
</template>
