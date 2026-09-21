<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useConfigurations } from '@/stores/configurations'
import ConfigurationDrawer from '@/components/ConfigurationDrawer.vue'
import type { Configuration } from '@/api/types'

const store = useConfigurations()
const drawer = ref(false)
const selected = ref<Configuration | null>(null)

onMounted(() => store.list())

function open(c: Configuration | null): void {
  selected.value = c
  drawer.value = true
}

const statusColor: Record<string, string> = { active: 'success', inactive: 'grey', error: 'error' }
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h5">Target configurations</h1>
      <v-spacer />
      <v-btn color="primary" prepend-icon="mdi-plus" data-test="config-new" @click="open(null)">New configuration</v-btn>
    </div>
    <v-alert v-if="store.error" type="error" variant="tonal" density="compact" class="mb-3">{{ store.error }}</v-alert>
    <v-table data-test="configs-table">
      <thead>
        <tr><th>Name</th><th>Provider</th><th>Status</th><th>Credentials</th><th>Last deployment</th></tr>
      </thead>
      <tbody>
        <tr v-for="c in store.items" :key="c.id" class="cursor-pointer" :data-test="'config-row-' + c.id" @click="open(c)">
          <td>{{ c.name }}</td>
          <td><v-chip size="x-small" variant="tonal">{{ c.provider_type }}</v-chip></td>
          <td><v-chip size="x-small" :color="statusColor[c.status]" variant="tonal">{{ c.status }}</v-chip></td>
          <td><v-icon v-if="c.has_credentials" icon="mdi-key" size="small" color="amber" /></td>
          <td class="text-medium-emphasis">{{ c.last_deployment_at ? new Date(c.last_deployment_at).toLocaleString() : '—' }}</td>
        </tr>
        <tr v-if="!store.items.length && !store.loading">
          <td colspan="5" class="text-medium-emphasis">No configurations yet.</td>
        </tr>
      </tbody>
    </v-table>
    <ConfigurationDrawer v-model="drawer" :configuration="selected" @saved="store.list()" @removed="store.list()" />
  </div>
</template>

<style scoped>
.cursor-pointer { cursor: pointer; }
</style>
