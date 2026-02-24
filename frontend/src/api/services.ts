/**
 * Deployer Module Service Functions
 *
 * Typed service methods for the Deployer API using dynamic module routing.
 * Base URL: /admin/v1/modules/deployer/v1
 */

import type { components, operations } from './types';
import { deployerApi, type RequestOptions } from './client';

// ==================== Entity Type Aliases ====================

export type DeploymentJob = components['schemas']['DeploymentJob'];
export type DeploymentTarget = components['schemas']['DeploymentTarget'];
export type TargetConfiguration = components['schemas']['TargetConfiguration'];
export type CertificateFilter = components['schemas']['CertificateFilter'];
export type ProviderInfo = components['schemas']['ProviderInfo'];
export type JobHistoryEntry = components['schemas']['JobHistoryEntry'];
export type JobStatistics = components['schemas']['JobStatistics'];
export type JobTimeBreakdown = components['schemas']['JobTimeBreakdown'];
export type TargetStatistics = components['schemas']['TargetStatistics'];
export type ConfigurationStatistics = components['schemas']['ConfigurationStatistics'];
export type TenantStatistics = components['schemas']['TenantStatistics'];
export type RecentError = components['schemas']['RecentError'];

// ==================== Enum Types Derived from Entity Fields ====================

export type DeploymentJobStatus = NonNullable<DeploymentJob['status']>;
export type DeploymentJobType = NonNullable<DeploymentJob['jobType']>;
export type DeploymentTriggerType = NonNullable<DeploymentJob['triggeredBy']>;
export type ConfigurationStatus = NonNullable<TargetConfiguration['status']>;

// ==================== Response Types ====================

// Deployment Job Responses
export type ListJobsResponse = components['schemas']['ListJobsResponse'];
export type CreateJobResponse = components['schemas']['CreateJobResponse'];
export type GetJobStatusResponse = components['schemas']['GetJobStatusResponse'];
export type GetJobResultResponse = components['schemas']['GetJobResultResponse'];
export type CancelJobResponse = components['schemas']['CancelJobResponse'];
export type RetryJobResponse = components['schemas']['RetryJobResponse'];

// Deployment Target Responses
export type ListTargetsResponse = components['schemas']['ListTargetsResponse'];
export type GetTargetResponse = components['schemas']['GetTargetResponse'];
export type CreateTargetResponse = components['schemas']['CreateTargetResponse'];
export type UpdateTargetResponse = components['schemas']['UpdateTargetResponse'];
export type ListTargetConfigurationsResponse = components['schemas']['ListTargetConfigurationsResponse'];
export type AddConfigurationsResponse = components['schemas']['AddConfigurationsResponse'];
export type RemoveConfigurationsResponse = components['schemas']['RemoveConfigurationsResponse'];

// Target Configuration Responses
export type ListConfigurationsResponse = components['schemas']['ListConfigurationsResponse'];
export type GetConfigurationResponse = components['schemas']['GetConfigurationResponse'];
export type CreateConfigurationResponse = components['schemas']['CreateConfigurationResponse'];
export type UpdateConfigurationResponse = components['schemas']['UpdateConfigurationResponse'];
export type ListConfigurationProvidersResponse = components['schemas']['ListConfigurationProvidersResponse'];
export type ValidateConfigurationCredentialsResponse = components['schemas']['ValidateConfigurationCredentialsResponse'];

// Statistics Responses
export type GetStatisticsResponse = components['schemas']['GetStatisticsResponse'];

// ==================== Request Types ====================

// Deployment Job Requests
export type CreateJobRequest = components['schemas']['CreateJobRequest'];

// Deployment Target Requests
export type CreateTargetRequest = components['schemas']['CreateTargetRequest'];
export type UpdateTargetRequest = components['schemas']['UpdateTargetRequest'];
export type AddConfigurationsRequest = components['schemas']['AddConfigurationsRequest'];

// Target Configuration Requests
export type CreateConfigurationRequest = components['schemas']['CreateConfigurationRequest'];
export type UpdateConfigurationRequest = components['schemas']['UpdateConfigurationRequest'];
export type ValidateConfigurationCredentialsRequest = components['schemas']['ValidateConfigurationCredentialsRequest'];

// ==================== Helper Functions ====================

function buildQuery(params: Record<string, unknown>): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      if (Array.isArray(value)) {
        value.forEach(v => searchParams.append(key, String(v)));
      } else {
        searchParams.append(key, String(value));
      }
    }
  }
  const query = searchParams.toString();
  return query ? `?${query}` : '';
}

// ==================== Deployment Job Service ====================

export const DeploymentJobService = {
  list: async (
    params?: operations['DeploymentJobService_ListJobs']['parameters']['query'],
    options?: RequestOptions
  ): Promise<ListJobsResponse> => {
    return deployerApi.get<ListJobsResponse>(`/deployment-jobs${buildQuery(params || {})}`, options);
  },

  create: async (
    data: CreateJobRequest,
    options?: RequestOptions
  ): Promise<CreateJobResponse> => {
    return deployerApi.post<CreateJobResponse>('/deployment-jobs', data, options);
  },

  getStatus: async (
    id: string,
    params?: operations['DeploymentJobService_GetJobStatus']['parameters']['query'],
    options?: RequestOptions
  ): Promise<GetJobStatusResponse> => {
    return deployerApi.get<GetJobStatusResponse>(`/deployment-jobs/${id}/status${buildQuery(params || {})}`, options);
  },

  getResult: async (
    id: string,
    params?: operations['DeploymentJobService_GetJobResult']['parameters']['query'],
    options?: RequestOptions
  ): Promise<GetJobResultResponse> => {
    return deployerApi.get<GetJobResultResponse>(`/deployment-jobs/${id}/result${buildQuery(params || {})}`, options);
  },

  cancel: async (
    id: string,
    params?: operations['DeploymentJobService_CancelJob']['parameters']['query'],
    options?: RequestOptions
  ): Promise<CancelJobResponse> => {
    return deployerApi.post<CancelJobResponse>(`/deployment-jobs/${id}/cancel${buildQuery(params || {})}`, undefined, options);
  },

  retry: async (
    id: string,
    params?: operations['DeploymentJobService_RetryJob']['parameters']['query'],
    options?: RequestOptions
  ): Promise<RetryJobResponse> => {
    return deployerApi.post<RetryJobResponse>(`/deployment-jobs/${id}/retry${buildQuery(params || {})}`, undefined, options);
  },
};

// ==================== Deployment Target Service ====================

export const DeploymentTargetService = {
  list: async (
    params?: operations['DeploymentTargetService_ListTargets']['parameters']['query'],
    options?: RequestOptions
  ): Promise<ListTargetsResponse> => {
    return deployerApi.get<ListTargetsResponse>(`/deployment-targets${buildQuery(params || {})}`, options);
  },

  get: async (
    id: string,
    params?: operations['DeploymentTargetService_GetTarget']['parameters']['query'],
    options?: RequestOptions
  ): Promise<GetTargetResponse> => {
    return deployerApi.get<GetTargetResponse>(`/deployment-targets/${id}${buildQuery(params || {})}`, options);
  },

  create: async (
    data: CreateTargetRequest,
    options?: RequestOptions
  ): Promise<CreateTargetResponse> => {
    return deployerApi.post<CreateTargetResponse>('/deployment-targets', data, options);
  },

  update: async (
    id: string,
    data: Omit<UpdateTargetRequest, 'id'>,
    options?: RequestOptions
  ): Promise<UpdateTargetResponse> => {
    return deployerApi.put<UpdateTargetResponse>(`/deployment-targets/${id}`, { ...data, id }, options);
  },

  delete: async (id: string, options?: RequestOptions): Promise<void> => {
    return deployerApi.delete<void>(`/deployment-targets/${id}`, options);
  },

  listConfigurations: async (
    id: string,
    params?: operations['DeploymentTargetService_ListTargetConfigurations']['parameters']['query'],
    options?: RequestOptions
  ): Promise<ListTargetConfigurationsResponse> => {
    return deployerApi.get<ListTargetConfigurationsResponse>(
      `/deployment-targets/${id}/configurations${buildQuery(params || {})}`,
      options
    );
  },

  addConfigurations: async (
    id: string,
    configurationIds: string[],
    options?: RequestOptions
  ): Promise<AddConfigurationsResponse> => {
    return deployerApi.post<AddConfigurationsResponse>(
      `/deployment-targets/${id}/configurations`,
      { id, configurationIds },
      options
    );
  },

  removeConfigurations: async (
    id: string,
    configurationIds: string[],
    options?: RequestOptions
  ): Promise<RemoveConfigurationsResponse> => {
    return deployerApi.delete<RemoveConfigurationsResponse>(
      `/deployment-targets/${id}/configurations${buildQuery({ configurationIds })}`,
      options
    );
  },
};

// ==================== Target Configuration Service ====================

export const TargetConfigurationService = {
  list: async (
    params?: operations['TargetConfigurationService_ListConfigurations']['parameters']['query'],
    options?: RequestOptions
  ): Promise<ListConfigurationsResponse> => {
    return deployerApi.get<ListConfigurationsResponse>(
      `/target-configurations${buildQuery(params || {})}`,
      options
    );
  },

  get: async (id: string, options?: RequestOptions): Promise<GetConfigurationResponse> => {
    return deployerApi.get<GetConfigurationResponse>(`/target-configurations/${id}`, options);
  },

  create: async (
    data: CreateConfigurationRequest,
    options?: RequestOptions
  ): Promise<CreateConfigurationResponse> => {
    return deployerApi.post<CreateConfigurationResponse>('/target-configurations', data, options);
  },

  update: async (
    id: string,
    data: Omit<UpdateConfigurationRequest, 'id'>,
    options?: RequestOptions
  ): Promise<UpdateConfigurationResponse> => {
    return deployerApi.put<UpdateConfigurationResponse>(`/target-configurations/${id}`, { ...data, id }, options);
  },

  delete: async (id: string, options?: RequestOptions): Promise<void> => {
    return deployerApi.delete<void>(`/target-configurations/${id}`, options);
  },

  listProviders: async (options?: RequestOptions): Promise<ListConfigurationProvidersResponse> => {
    return deployerApi.get<ListConfigurationProvidersResponse>('/target-configurations/providers', options);
  },

  validateCredentials: async (
    data: ValidateConfigurationCredentialsRequest,
    options?: RequestOptions
  ): Promise<ValidateConfigurationCredentialsResponse> => {
    return deployerApi.post<ValidateConfigurationCredentialsResponse>(
      '/target-configurations/validate-credentials',
      data,
      options
    );
  },
};

// ==================== Statistics Service ====================

export const DeployerStatisticsService = {
  getStatistics: async (
    params?: operations['DeployerStatisticsService_GetStatistics']['parameters']['query'],
    options?: RequestOptions
  ): Promise<GetStatisticsResponse> => {
    // Note: This endpoint uses /api/v1/deployer path prefix
    return deployerApi.get<GetStatisticsResponse>(`/api/v1/deployer/statistics${buildQuery(params || {})}`, options);
  },

  getTenantStatistics: async (
    tenantId: number,
    options?: RequestOptions
  ): Promise<TenantStatistics> => {
    // Note: This endpoint uses /api/v1/deployer path prefix
    return deployerApi.get<TenantStatistics>(
      `/api/v1/deployer/statistics/tenant/${tenantId}`,
      options
    );
  },
};
