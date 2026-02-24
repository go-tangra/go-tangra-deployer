import { defineStore } from 'pinia';

import {
  TargetConfigurationService,
  type ListConfigurationsResponse,
  type GetConfigurationResponse,
  type CreateConfigurationResponse,
  type UpdateConfigurationResponse,
  type CreateConfigurationRequest,
  type UpdateConfigurationRequest,
  type ConfigurationStatus,
  type ListConfigurationProvidersResponse,
} from '../api/services';

interface Paging { page: number; pageSize: number; }

export const useDeployerConfigurationStore = defineStore(
  'deployer-configuration',
  () => {
    /**
     * List target configurations
     */
    async function listConfigurations(
      paging?: Paging,
      formValues?: {
        providerType?: string;
        status?: ConfigurationStatus;
        tenantId?: number;
      } | null,
    ): Promise<ListConfigurationsResponse> {
      return await TargetConfigurationService.list({
        tenantId: formValues?.tenantId,
        providerType: formValues?.providerType,
        status: formValues?.status,
        page: paging?.page,
        pageSize: paging?.pageSize,
      });
    }

    /**
     * Get configuration by ID
     */
    async function getConfiguration(id: string): Promise<GetConfigurationResponse> {
      return await TargetConfigurationService.get(id);
    }

    /**
     * Create a new target configuration
     */
    async function createConfiguration(
      request: CreateConfigurationRequest,
    ): Promise<CreateConfigurationResponse> {
      return await TargetConfigurationService.create(request);
    }

    /**
     * Update a target configuration
     */
    async function updateConfiguration(
      id: string,
      request: Omit<UpdateConfigurationRequest, 'id'>,
    ): Promise<UpdateConfigurationResponse> {
      return await TargetConfigurationService.update(id, request);
    }

    /**
     * Delete a target configuration
     */
    async function deleteConfiguration(id: string): Promise<void> {
      return await TargetConfigurationService.delete(id);
    }

    /**
     * List available configuration providers
     */
    async function listProviders(): Promise<ListConfigurationProvidersResponse> {
      return await TargetConfigurationService.listProviders();
    }

    function $reset() {}

    return {
      $reset,
      listConfigurations,
      getConfiguration,
      createConfiguration,
      updateConfiguration,
      deleteConfiguration,
      listProviders,
    };
  },
);
