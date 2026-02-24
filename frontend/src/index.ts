import type { TangraModule } from './sdk';
import routes from './routes';
import { useDeployerTargetStore } from './stores/deployer-target.state';
import { useDeployerConfigurationStore } from './stores/deployer-configuration.state';
import { useDeployerJobStore } from './stores/deployer-job.state';
import enUS from './locales/en-US.json';

const deployerModule: TangraModule = {
  id: 'deployer',
  version: '1.0.0',
  routes,
  stores: {
    'deployer-target': useDeployerTargetStore,
    'deployer-configuration': useDeployerConfigurationStore,
    'deployer-job': useDeployerJobStore,
  },
  locales: {
    'en-US': enUS,
  },
};

export default deployerModule;
