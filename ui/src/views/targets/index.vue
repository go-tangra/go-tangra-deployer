<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useTargets } from '@/stores/targets'
import TargetDrawer from '@/components/TargetDrawer.vue'
import type { Target } from '@/api/types'

const store = useTargets()
const drawer = ref(false)
const selected = ref<Target | null>(null)

onMounted(() => store.list())

function open(t: Target | null): void {
  selected.value = t
  drawer.value = true
}
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h5">Deployment targets</h1>
      <v-spacer />
      <v-btn color="primary" prepend-icon="mdi-plus" data-test="target-new" @click="open(null)">New target</v-btn>
    </div>
    <v-alert v-if="store.error" type="error" variant="tonal" density="compact" class="mb-3">{{ store.error }}</v-alert>
    <v-table data-test="targets-table">
      <thead>
        <tr><th>Name</th><th>Auto-deploy</th><th>Filters</th><th>Configurations</th></tr>
      </thead>
      <tbody>
        <tr v-for="t in store.items" :key="t.id" class="cursor-pointer" :data-test="'target-row-' + t.id" @click="open(t)">
          <td>{{ t.name }}</td>
          <td>
            <v-chip size="x-small" :color="t.auto_deploy ? 'success' : undefined" variant="tonal">
              {{ t.auto_deploy ? 'on' : 'off' }}
            </v-chip>
          </td>
          <td>{{ t.certificate_filters?.length || 0 }}</td>
          <td>{{ t.configuration_ids?.length || 0 }}</td>
        </tr>
        <tr v-if="!store.items.length && !store.loading">
          <td colspan="4" class="text-medium-emphasis">No targets yet.</td>
        </tr>
      </tbody>
    </v-table>
    <TargetDrawer v-model="drawer" :target="selected" @saved="store.list()" @removed="store.list()" />
  </div>
</template>

<style scoped>
.cursor-pointer { cursor: pointer; }
</style>
