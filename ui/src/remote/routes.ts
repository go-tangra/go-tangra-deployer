import type { RouteRecordRaw } from 'vue-router'

// Routes mounted by the platform shell under their own error boundary.
export const routes: RouteRecordRaw[] = [
  { path: '/deployer', name: 'deployer-dashboard', component: () => import('@/views/dashboard/index.vue'), meta: { module: 'deployer' } },
  { path: '/deployer/targets', name: 'deployer-targets', component: () => import('@/views/targets/index.vue'), meta: { module: 'deployer' } },
  { path: '/deployer/configurations', name: 'deployer-configurations', component: () => import('@/views/configurations/index.vue'), meta: { module: 'deployer' } },
  { path: '/deployer/jobs', name: 'deployer-jobs', component: () => import('@/views/jobs/index.vue'), meta: { module: 'deployer' } },
]
export default routes
