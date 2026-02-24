import { defineStore } from 'pinia';

import {
  DeploymentTargetService,
  type ListTargetsResponse,
  type GetTargetResponse,
  type CreateTargetResponse,
  type UpdateTargetResponse,
  type ListTargetConfigurationsResponse,
  type CreateTargetRequest,
  type UpdateTargetRequest,
} from '../api/services';

interface Paging { page: number; pageSize: number; }

export const useDeployerTargetStore = defineStore('deployer-target', () => {
  /**
   * List deployment targets (groups)
   */
  async function listTargets(
    paging?: Paging,
    formValues?: {
      autoDeployOnRenewal?: boolean;
      tenantId?: number;
      includeConfigurations?: boolean;
    } | null,
  ): Promise<ListTargetsResponse> {
    return await DeploymentTargetService.list({
      tenantId: formValues?.tenantId,
      autoDeployOnRenewal: formValues?.autoDeployOnRenewal,
      includeConfigurations: formValues?.includeConfigurations,
      page: paging?.page,
      pageSize: paging?.pageSize,
    });
  }

  /**
   * Get target by ID
   */
  async function getTarget(
    id: string,
    includeConfigurations?: boolean,
  ): Promise<GetTargetResponse> {
    return await DeploymentTargetService.get(id, { includeConfigurations });
  }

  /**
   * Create a new deployment target (group)
   */
  async function createTarget(
    request: CreateTargetRequest,
  ): Promise<CreateTargetResponse> {
    return await DeploymentTargetService.create(request);
  }

  /**
   * Update a deployment target
   */
  async function updateTarget(
    id: string,
    request: Omit<UpdateTargetRequest, 'id'>,
  ): Promise<UpdateTargetResponse> {
    return await DeploymentTargetService.update(id, request);
  }

  /**
   * Delete a deployment target
   */
  async function deleteTarget(id: string): Promise<void> {
    return await DeploymentTargetService.delete(id);
  }

  /**
   * Add configurations to a target group
   */
  async function addConfigurations(
    id: string,
    configurationIds: string[],
  ): Promise<void> {
    await DeploymentTargetService.addConfigurations(id, configurationIds);
  }

  /**
   * Remove configurations from a target group
   */
  async function removeConfigurations(
    id: string,
    configurationIds: string[],
  ): Promise<void> {
    await DeploymentTargetService.removeConfigurations(id, configurationIds);
  }

  /**
   * List configurations linked to a target group
   */
  async function listTargetConfigurations(
    id: string,
    paging?: Paging,
  ): Promise<ListTargetConfigurationsResponse> {
    return await DeploymentTargetService.listConfigurations(id, {
      page: paging?.page,
      pageSize: paging?.pageSize,
    });
  }

  function $reset() {}

  return {
    $reset,
    listTargets,
    getTarget,
    createTarget,
    updateTarget,
    deleteTarget,
    addConfigurations,
    removeConfigurations,
    listTargetConfigurations,
  };
});
