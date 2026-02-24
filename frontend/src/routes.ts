import type { RouteRecordRaw } from 'vue-router';

const routes: RouteRecordRaw[] = [
  {
    path: '/deployer',
    name: 'Deployer',
    component: () => import('shell/app-layout'),
    redirect: '/deployer/target',
    meta: {
      order: 2007,
      icon: 'lucide:rocket',
      title: 'deployer.menu.moduleName',
      keepAlive: true,
      authority: ['platform:admin', 'tenant:manager'],
    },
    children: [
      {
        path: 'target',
        name: 'DeploymentTargetManagement',
        meta: {
          icon: 'lucide:target',
          title: 'deployer.menu.target',
          authority: ['platform:admin', 'tenant:manager'],
        },
        component: () => import('./views/target/index.vue'),
      },
      {
        path: 'configuration',
        name: 'TargetConfigurationManagement',
        meta: {
          icon: 'lucide:settings',
          title: 'deployer.menu.configuration',
          authority: ['platform:admin', 'tenant:manager'],
        },
        component: () => import('./views/configuration/index.vue'),
      },
      {
        path: 'job',
        name: 'DeploymentJobManagement',
        meta: {
          icon: 'lucide:briefcase',
          title: 'deployer.menu.job',
          authority: ['platform:admin', 'tenant:manager'],
        },
        component: () => import('./views/job/index.vue'),
      },
    ],
  },
];

export default routes;
