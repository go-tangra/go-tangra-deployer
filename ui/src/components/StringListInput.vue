<script setup lang="ts">
// A list of strings as chips (descriptor type string_list): type a value and
// press Enter (or leave the field) to add it; the chip button removes it.
// v-model is string[]. Item rules (pattern, length) are checked by the form
// schema generated from the descriptor.
import { computed, ref } from 'vue'
import { UiField, UiIcon } from '@go-tangra/ui'

const props = defineProps<{ modelValue?: unknown | undefined; id: string; label: string; hint?: string | undefined; error?: string | undefined; required?: boolean | undefined; placeholder?: string | undefined; max?: number | undefined; disabled?: boolean | undefined }>()
const emit = defineEmits<{ (e: 'update:modelValue', v: string[]): void; (e: 'blur'): void }>()
const items = computed<string[]>(() => (Array.isArray(props.modelValue) ? props.modelValue.map(String) : []))
const draft = ref('')
const full = computed(() => !!props.max && items.value.length >= props.max)

function add(): void {
  const s = draft.value.trim()
  if (!s || full.value) return
  if (!items.value.includes(s)) emit('update:modelValue', [...items.value, s])
  draft.value = ''
}
function remove(i: number): void {
  emit('update:modelValue', items.value.filter((_, j) => j !== i))
}
function onBackspace(): void {
  if (draft.value === '' && items.value.length) remove(items.value.length - 1)
}
</script>

<template>
  <UiField :id="id" :label="label" :hint="hint ?? 'Enter to add'" :error="error" :required="required">
    <template #default="{ describedBy, invalid }">
      <div class="flex min-h-10 flex-wrap items-center gap-1 rounded-field border bg-base-100 p-1" :class="invalid ? 'border-error' : 'border-base-content/20'">
        <span v-for="(it, i) in items" :key="it" class="badge badge-soft badge-primary max-w-full gap-1" data-test="list-item">
          <span class="truncate">{{ it }}</span>
          <button v-if="!disabled" type="button" class="ms-0.5" :aria-label="'Remove ' + it" @click="remove(i)"><UiIcon name="mdi-close" size="xs" /></button>
        </span>
        <input
          :id="id"
          :data-field="id"
          class="min-w-32 grow border-0 bg-transparent px-1 py-1 text-sm outline-none"
          :value="draft"
          :placeholder="full ? '' : placeholder"
          :disabled="disabled || full"
          :aria-required="required || undefined"
          :aria-invalid="invalid || undefined"
          :aria-describedby="describedBy"
          autocomplete="off"
          @input="draft = ($event.target as HTMLInputElement).value"
          @keydown.enter.prevent="add"
          @keydown.backspace="onBackspace"
          @blur="add(); emit('blur')"
        >
      </div>
    </template>
  </UiField>
</template>
