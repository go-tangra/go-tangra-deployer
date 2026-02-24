import { defineStore } from 'pinia';

import {
  DeploymentJobService,
  type ListJobsResponse,
  type CreateJobResponse,
  type GetJobStatusResponse,
  type GetJobResultResponse,
  type CancelJobResponse,
  type RetryJobResponse,
  type CreateJobRequest,
  type DeploymentJobStatus,
  type DeploymentTriggerType,
} from '../api/services';

interface Paging { page: number; pageSize: number; }

export const useDeployerJobStore = defineStore('deployer-job', () => {
  /**
   * List deployment jobs
   */
  async function listJobs(
    paging?: Paging,
    formValues?: {
      certificateId?: string;
      createdAfter?: string;
      createdBefore?: string;
      status?: DeploymentJobStatus;
      deploymentTargetId?: string;
      targetConfigurationId?: string;
      tenantId?: number;
      triggeredBy?: DeploymentTriggerType;
    } | null,
  ): Promise<ListJobsResponse> {
    return await DeploymentJobService.list({
      tenantId: formValues?.tenantId,
      deploymentTargetId: formValues?.deploymentTargetId,
      targetConfigurationId: formValues?.targetConfigurationId,
      certificateId: formValues?.certificateId,
      status: formValues?.status,
      triggeredBy: formValues?.triggeredBy,
      createdAfter: formValues?.createdAfter,
      createdBefore: formValues?.createdBefore,
      page: paging?.page,
      pageSize: paging?.pageSize,
    });
  }

  /**
   * Get job status by ID
   */
  async function getJobStatus(id: string): Promise<GetJobStatusResponse> {
    return await DeploymentJobService.getStatus(id);
  }

  /**
   * Get job result with history
   */
  async function getJobResult(id: string): Promise<GetJobResultResponse> {
    return await DeploymentJobService.getResult(id);
  }

  /**
   * Create a new deployment job
   */
  async function createJob(
    request: CreateJobRequest,
  ): Promise<CreateJobResponse> {
    return await DeploymentJobService.create(request);
  }

  /**
   * Cancel a job
   */
  async function cancelJob(id: string): Promise<CancelJobResponse> {
    return await DeploymentJobService.cancel(id);
  }

  /**
   * Retry a failed job
   */
  async function retryJob(id: string): Promise<RetryJobResponse> {
    return await DeploymentJobService.retry(id);
  }

  function $reset() {}

  return {
    $reset,
    listJobs,
    getJobStatus,
    getJobResult,
    createJob,
    cancelJob,
    retryJob,
  };
});
