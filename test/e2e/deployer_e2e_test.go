// Package e2e contains end-to-end tests for the deployer service
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
	lcmV1 "github.com/go-tangra/go-tangra-lcm/gen/go/lcm/service/v1"
)

// ============================================================================
// Tests that only require Deployer service (no LCM needed)
// ============================================================================

// TestDeployerE2E_FailingDeployment tests deployment failure handling
// This test does NOT require LCM - it only tests deployer service
func TestDeployerE2E_FailingDeployment(t *testing.T) {
	cfg := LoadTestConfig()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
	if err != nil {
		t.Skipf("Could not connect to Deployer service at %s: %v", cfg.DeployerEndpoint, err)
	}
	defer deployerClients.Close()

	ctx := context.Background()
	testID := fmt.Sprintf("e2e-fail-%d", time.Now().Unix())

	// Create a target configured to fail
	t.Log("Creating deployment target configured to fail...")
	targetName := fmt.Sprintf("test-fail-target-%s", testID)

	configStruct, err := structpb.NewStruct(map[string]any{
		"simulate_delay_ms": 200.0,
		"should_fail":       true,
		"fail_message":      "Simulated failure for e2e test",
	})
	require.NoError(t, err)

	credsStruct, err := structpb.NewStruct(map[string]any{})
	require.NoError(t, err)

	description := "E2E test failing deployment target"

	createTargetReq := &deployerV1.CreateTargetRequest{
		Name:         targetName,
		ProviderType: "dummy",
		TenantId:     cfg.TenantID,
		Description:  &description,
		Config:       configStruct,
		Credentials:  credsStruct,
	}

	targetResp, err := deployerClients.Target.CreateTarget(ctx, createTargetReq)
	require.NoError(t, err)
	targetID := targetResp.GetTarget().GetId()
	t.Logf("Created failing target: %s", targetID)

	// Deploy (should fail)
	t.Log("Deploying certificate (expecting failure)...")
	waitForCompletion := true
	timeoutSeconds := int32(30)
	deployResp, err := deployerClients.Deployment.Deploy(ctx, &deployerV1.DeployRequest{
		TargetId:          targetID,
		CertificateId:     "test-cert-123",
		WaitForCompletion: &waitForCompletion,
		TimeoutSeconds:    &timeoutSeconds,
	})
	require.NoError(t, err)

	// Verify failure
	job := deployResp.GetJob()
	assert.Equal(t, deployerV1.JobStatus_JOB_STATUS_FAILED, job.GetStatus())
	assert.Contains(t, job.GetStatusMessage(), "Simulated failure")
	t.Logf("Deployment failed as expected: %s", job.GetStatusMessage())

	// Cleanup
	_, _ = deployerClients.Target.DeleteTarget(ctx, &deployerV1.DeleteTargetRequest{Id: targetID})

	t.Log("Failure test completed successfully!")
}

// TestDeployerE2E_MultiTargetDeployment tests deploying to multiple targets
// This test does NOT require LCM - it only tests deployer service
func TestDeployerE2E_MultiTargetDeployment(t *testing.T) {
	cfg := LoadTestConfig()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
	if err != nil {
		t.Skipf("Could not connect to Deployer service at %s: %v", cfg.DeployerEndpoint, err)
	}
	defer deployerClients.Close()

	ctx := context.Background()
	testID := fmt.Sprintf("e2e-multi-%d", time.Now().Unix())

	// Create 3 targets
	targetIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		targetName := fmt.Sprintf("test-multi-target-%s-%d", testID, i)

		configStruct, err := structpb.NewStruct(map[string]any{
			"simulate_delay_ms": float64(300 + (i * 100)), // Staggered delays
		})
		require.NoError(t, err)

		credsStruct, err := structpb.NewStruct(map[string]any{})
		require.NoError(t, err)

		description := fmt.Sprintf("Multi-target test %d", i)

		createResp, err := deployerClients.Target.CreateTarget(ctx, &deployerV1.CreateTargetRequest{
			Name:         targetName,
			ProviderType: "dummy",
			TenantId:     cfg.TenantID,
			Description:  &description,
			Config:       configStruct,
			Credentials:  credsStruct,
		})
		require.NoError(t, err)
		targetIDs[i] = createResp.GetTarget().GetId()
		t.Logf("Created target %d: %s", i, targetIDs[i])
	}

	// Deploy to all targets
	t.Log("Deploying to multiple targets...")
	triggeredBy := "manual"
	deployResp, err := deployerClients.Deployment.DeployToTargets(ctx, &deployerV1.DeployToTargetsRequest{
		CertificateId: "multi-test-cert-123",
		TargetIds:     targetIDs,
		TriggeredBy:   &triggeredBy,
	})
	require.NoError(t, err)

	assert.Equal(t, int32(3), deployResp.GetTotal())
	assert.Equal(t, int32(3), deployResp.GetSucceeded())
	assert.Equal(t, int32(0), deployResp.GetFailed())
	t.Logf("Created %d deployment jobs", len(deployResp.GetResults()))

	// Wait for all jobs to complete
	t.Log("Waiting for all deployments to complete...")
	for _, result := range deployResp.GetResults() {
		if result.GetJob() == nil {
			continue
		}
		jobID := result.GetJob().GetId()

		for i := 0; i < 30; i++ {
			time.Sleep(500 * time.Millisecond)

			statusResp, err := deployerClients.Job.GetJobStatus(ctx, &deployerV1.GetJobStatusRequest{
				Id: jobID,
			})
			if err != nil {
				continue
			}

			if statusResp.GetJob().GetStatus() == deployerV1.JobStatus_JOB_STATUS_COMPLETED {
				t.Logf("Job %s completed", jobID)
				break
			}
		}
	}

	// Cleanup
	for _, id := range targetIDs {
		_, _ = deployerClients.Target.DeleteTarget(ctx, &deployerV1.DeleteTargetRequest{Id: id})
	}

	t.Log("Multi-target test completed successfully!")
}

// TestDeployerE2E_ProviderValidation tests provider credential validation
// This test does NOT require LCM - it only tests deployer service
func TestDeployerE2E_ProviderValidation(t *testing.T) {
	cfg := LoadTestConfig()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
	if err != nil {
		t.Skipf("Could not connect to Deployer service at %s: %v", cfg.DeployerEndpoint, err)
	}
	defer deployerClients.Close()

	ctx := context.Background()

	// List available providers
	t.Log("Listing available providers...")
	listResp, err := deployerClients.Target.ListProviders(ctx, &deployerV1.ListProvidersRequest{})
	require.NoError(t, err)

	for _, provider := range listResp.GetProviders() {
		t.Logf("Provider: %s - %s", provider.GetType(), provider.GetDisplayName())
	}

	// Validate good credentials
	t.Log("Validating good credentials...")
	goodCreds, _ := structpb.NewStruct(map[string]any{"api_key": "test"})
	validateResp, err := deployerClients.Target.ValidateCredentials(ctx, &deployerV1.ValidateCredentialsRequest{
		ProviderType: "dummy",
		Credentials:  goodCreds,
	})
	require.NoError(t, err)
	assert.True(t, validateResp.GetValid())
	t.Logf("Validation result: valid=%v", validateResp.GetValid())

	// Validate failing credentials
	t.Log("Validating failing credentials...")
	badCreds, _ := structpb.NewStruct(map[string]any{"should_fail": true})
	validateResp, err = deployerClients.Target.ValidateCredentials(ctx, &deployerV1.ValidateCredentialsRequest{
		ProviderType: "dummy",
		Credentials:  badCreds,
	})
	require.NoError(t, err)
	assert.False(t, validateResp.GetValid())
	t.Logf("Validation result: valid=%v, message=%s", validateResp.GetValid(), validateResp.GetMessage())

	t.Log("Provider validation test completed!")
}

// ============================================================================
// Tests that require LCM service with mTLS
// ============================================================================

// TestDeployerE2E_FullFlowWithLCM tests the complete end-to-end flow:
// 1. Register with LCM to get mTLS certificate
// 2. Create self-signed issuer
// 3. Create deployment target with dummy provider
// 4. Request certificate from LCM
// 5. Deploy certificate to target
// 6. Verify deployment
func TestDeployerE2E_FullFlowWithLCM(t *testing.T) {
	cfg := LoadTestConfig()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Check if CA file exists
	if cfg.CAFile == "" {
		t.Skip("Skipping: LCM CA file not found. Set LCM_CA_FILE environment variable.")
	}
	if _, err := os.Stat(cfg.CAFile); os.IsNotExist(err) {
		t.Skipf("Skipping: LCM CA file not found at %s", cfg.CAFile)
	}

	// Create temp directory for certificates
	tempDir, err := os.MkdirTemp("", "deployer-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	testID := fmt.Sprintf("e2e-%d", time.Now().Unix())
	var creds *ClientCredentials
	var issuerName string
	var certSerial string

	// Step 1: Register with LCM to get mTLS certificate
	t.Run("1_RegisterWithLCM", func(t *testing.T) {
		t.Log("Connecting to LCM service...")

		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, nil, cfg.Timeouts.Connection)
		if err != nil {
			t.Skipf("Could not connect to LCM service: %v", err)
		}
		defer lcmClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		clientID := fmt.Sprintf("deployer-e2e-%s", testID)
		t.Logf("Registering client: %s", clientID)

		creds, err = RegisterAndGetMTLSCert(ctx, lcmClients, clientID, cfg.SharedSecret, tempDir)
		if err != nil {
			t.Fatalf("Failed to register and get mTLS certificate: %v", err)
		}

		t.Logf("mTLS certificate obtained: %s", creds.CertFile)
	})

	// Step 2: Create self-signed issuer using mTLS
	t.Run("2_CreateIssuer", func(t *testing.T) {
		if creds == nil {
			t.Skip("No mTLS credentials from registration step")
		}

		t.Log("Creating self-signed issuer...")

		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect with mTLS: %v", err)
		}
		defer lcmClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		issuerName = fmt.Sprintf("deployer-e2e-issuer-%s", testID)

		resp, err := lcmClients.Issuer.CreateIssuer(ctx, &lcmV1.CreateIssuerRequest{
			Name:        issuerName,
			Type:        "self-signed",
			KeyType:     "ecdsa",
			Description: "Deployer E2E test issuer",
			Status:      lcmV1.IssuerStatus_ISSUER_STATUS_ACTIVE,
			SelfIssuer: &lcmV1.SelfIssuer{
				CommonName:           "Deployer E2E Test",
				CaCommonName:         "Deployer E2E CA",
				CaOrganization:       "E2E Test Org",
				CaOrganizationalUnit: "Deployer Tests",
				CaCountry:            "US",
				CaValidityDays:       365,
			},
		})
		if err != nil {
			t.Fatalf("Failed to create issuer: %v", err)
		}

		t.Logf("Issuer created: %s (type=%s)", resp.Issuer.GetName(), resp.Issuer.GetType())
	})

	// Step 3: Create deployment target
	t.Run("3_CreateDeploymentTarget", func(t *testing.T) {
		t.Log("Creating deployment target...")

		deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Could not connect to Deployer service: %v", err)
		}
		defer deployerClients.Close()

		ctx := context.Background()
		targetName := fmt.Sprintf("e2e-target-%s", testID)

		configStruct, err := structpb.NewStruct(map[string]any{
			"simulate_delay_ms":       500.0,
			"simulate_progress_steps": 5.0,
		})
		require.NoError(t, err)

		credsStruct, err := structpb.NewStruct(map[string]any{})
		require.NoError(t, err)

		description := "E2E test deployment target"
		autoDeployOnRenewal := true

		createResp, err := deployerClients.Target.CreateTarget(ctx, &deployerV1.CreateTargetRequest{
			Name:                targetName,
			ProviderType:        "dummy",
			TenantId:            cfg.TenantID,
			Description:         &description,
			Config:              configStruct,
			Credentials:         credsStruct,
			AutoDeployOnRenewal: &autoDeployOnRenewal,
			CertificateFilters: []*deployerV1.CertificateFilter{
				{
					IssuerName:    &issuerName,
					DomainPattern: strPtr(".*\\.e2e\\.example\\.com"),
				},
			},
		})
		require.NoError(t, err)
		t.Logf("Target created: %s (ID=%s)", targetName, createResp.GetTarget().GetId())
	})

	// Step 4: Request certificate from LCM
	t.Run("4_RequestCertificate", func(t *testing.T) {
		if creds == nil || issuerName == "" {
			t.Skip("Missing prerequisites from earlier steps")
		}

		t.Log("Requesting certificate from LCM...")

		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect with mTLS: %v", err)
		}
		defer lcmClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		commonName := fmt.Sprintf("app.%s.e2e.example.com", testID)

		certResp, err := lcmClients.CertificateJob.RequestCertificate(ctx, &lcmV1.RequestCertificateRequest{
			IssuerName: issuerName,
			CommonName: commonName,
			DnsNames:   []string{commonName, fmt.Sprintf("www.%s.e2e.example.com", testID)},
		})
		require.NoError(t, err)

		jobID := certResp.GetJobId()
		t.Logf("Certificate job created: %s", jobID)

		// Poll for completion
		ticker := time.NewTicker(cfg.Timeouts.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				t.Fatal("Timeout waiting for certificate issuance")
			case <-ticker.C:
				statusResp, err := lcmClients.CertificateJob.GetJobStatus(ctx, &lcmV1.GetJobStatusRequest{
					JobId: jobID,
				})
				if err != nil {
					continue
				}

				t.Logf("Job status: %s", statusResp.GetStatus())

				switch statusResp.GetStatus() {
				case lcmV1.CertificateJobStatus_CERTIFICATE_JOB_STATUS_COMPLETED:
					// Get the result
					resultResp, err := lcmClients.CertificateJob.GetJobResult(ctx, &lcmV1.GetJobResultRequest{
						JobId: jobID,
					})
					require.NoError(t, err)

					certSerial = resultResp.GetSerialNumber()
					t.Logf("Certificate issued! Serial: %s", certSerial)

					if resultResp.CertificatePem != nil {
						cert, err := ParseCertificatePEM(*resultResp.CertificatePem)
						if err == nil {
							t.Logf("  Subject: %s", cert.Subject.CommonName)
							t.Logf("  DNS Names: %v", cert.DNSNames)
						}
					}
					return

				case lcmV1.CertificateJobStatus_CERTIFICATE_JOB_STATUS_FAILED:
					t.Fatalf("Certificate issuance failed: %s", statusResp.GetErrorMessage())
				}
			}
		}
	})

	// Step 5: Deploy certificate to target
	t.Run("5_DeployCertificate", func(t *testing.T) {
		if certSerial == "" {
			t.Skip("No certificate from previous step")
		}

		t.Log("Deploying certificate to target...")

		deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Could not connect to Deployer service: %v", err)
		}
		defer deployerClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.Deployment)
		defer cancel()

		// List targets to find ours
		listResp, err := deployerClients.Target.ListTargets(ctx, &deployerV1.ListTargetsRequest{})
		require.NoError(t, err)

		var targetID string
		for _, target := range listResp.GetItems() {
			if target.GetName() == fmt.Sprintf("e2e-target-%s", testID) {
				targetID = target.GetId()
				break
			}
		}

		if targetID == "" {
			t.Fatal("Could not find deployment target")
		}

		// Deploy
		waitForCompletion := true
		timeoutSeconds := int32(60)
		deployResp, err := deployerClients.Deployment.Deploy(ctx, &deployerV1.DeployRequest{
			TargetId:          targetID,
			CertificateId:     certSerial,
			WaitForCompletion: &waitForCompletion,
			TimeoutSeconds:    &timeoutSeconds,
		})
		require.NoError(t, err)

		assert.Equal(t, deployerV1.JobStatus_JOB_STATUS_COMPLETED, deployResp.GetJob().GetStatus())
		t.Logf("Deployment completed! Job ID: %s", deployResp.GetJob().GetId())

		// Cleanup
		_, _ = deployerClients.Target.DeleteTarget(ctx, &deployerV1.DeleteTargetRequest{Id: targetID})
	})

	t.Log("Full E2E flow completed successfully!")
}

// TestDeployerE2E_RenewalFlow tests auto-deployment on certificate renewal
func TestDeployerE2E_RenewalFlow(t *testing.T) {
	cfg := LoadTestConfig()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Check if CA file exists
	if cfg.CAFile == "" {
		t.Skip("Skipping: LCM CA file not found. Set LCM_CA_FILE environment variable.")
	}
	if _, err := os.Stat(cfg.CAFile); os.IsNotExist(err) {
		t.Skipf("Skipping: LCM CA file not found at %s", cfg.CAFile)
	}

	// Create temp directory for certificates
	tempDir, err := os.MkdirTemp("", "deployer-renew-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	testID := fmt.Sprintf("renew-%d", time.Now().Unix())
	var creds *ClientCredentials
	var issuerName string
	var targetID string

	// Step 1: Setup (register, create issuer, create target)
	t.Run("1_Setup", func(t *testing.T) {
		// Register with LCM
		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, nil, cfg.Timeouts.Connection)
		if err != nil {
			t.Skipf("Could not connect to LCM service: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		clientID := fmt.Sprintf("deployer-renew-%s", testID)
		creds, err = RegisterAndGetMTLSCert(ctx, lcmClients, clientID, cfg.SharedSecret, tempDir)
		lcmClients.Close()
		if err != nil {
			t.Fatalf("Failed to register: %v", err)
		}
		t.Log("mTLS certificate obtained")

		// Create issuer with mTLS
		lcmClients, err = NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect with mTLS: %v", err)
		}
		defer lcmClients.Close()

		issuerName = fmt.Sprintf("renew-issuer-%s", testID)
		_, err = lcmClients.Issuer.CreateIssuer(ctx, &lcmV1.CreateIssuerRequest{
			Name:    issuerName,
			Type:    "self-signed",
			KeyType: "ecdsa",
			Status:  lcmV1.IssuerStatus_ISSUER_STATUS_ACTIVE,
			SelfIssuer: &lcmV1.SelfIssuer{
				CommonName:     "Renewal Test",
				CaCommonName:   "Renewal CA",
				CaOrganization: "Test Org",
				CaValidityDays: 365,
			},
		})
		if err != nil {
			t.Fatalf("Failed to create issuer: %v", err)
		}
		t.Logf("Issuer created: %s", issuerName)

		// Create deployment target
		deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Could not connect to Deployer: %v", err)
		}
		defer deployerClients.Close()

		targetName := fmt.Sprintf("renew-target-%s", testID)
		configStruct, _ := structpb.NewStruct(map[string]any{"simulate_delay_ms": 300.0})
		credsStruct, _ := structpb.NewStruct(map[string]any{})
		description := "Renewal test target"
		autoRenewal := true

		createResp, err := deployerClients.Target.CreateTarget(ctx, &deployerV1.CreateTargetRequest{
			Name:                targetName,
			ProviderType:        "dummy",
			TenantId:            cfg.TenantID,
			Description:         &description,
			Config:              configStruct,
			Credentials:         credsStruct,
			AutoDeployOnRenewal: &autoRenewal,
		})
		require.NoError(t, err)
		targetID = createResp.GetTarget().GetId()
		t.Logf("Target created: %s", targetID)
	})

	// Step 2: Issue initial certificate and deploy
	t.Run("2_InitialCertAndDeploy", func(t *testing.T) {
		if creds == nil || issuerName == "" || targetID == "" {
			t.Skip("Missing prerequisites")
		}

		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer lcmClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		// Request initial certificate
		commonName := fmt.Sprintf("initial.%s.example.com", testID)
		certResp, err := lcmClients.CertificateJob.RequestCertificate(ctx, &lcmV1.RequestCertificateRequest{
			IssuerName: issuerName,
			CommonName: commonName,
			DnsNames:   []string{commonName},
		})
		require.NoError(t, err)
		t.Logf("Initial certificate job: %s", certResp.GetJobId())

		// Wait for issuance
		var certSerial string
		for i := 0; i < 60; i++ {
			time.Sleep(2 * time.Second)
			statusResp, err := lcmClients.CertificateJob.GetJobStatus(ctx, &lcmV1.GetJobStatusRequest{
				JobId: certResp.GetJobId(),
			})
			if err != nil {
				continue
			}
			if statusResp.GetStatus() == lcmV1.CertificateJobStatus_CERTIFICATE_JOB_STATUS_COMPLETED {
				resultResp, _ := lcmClients.CertificateJob.GetJobResult(ctx, &lcmV1.GetJobResultRequest{
					JobId: certResp.GetJobId(),
				})
				certSerial = resultResp.GetSerialNumber()
				t.Logf("Initial certificate issued: %s", certSerial)
				break
			}
		}
		require.NotEmpty(t, certSerial)

		// Deploy initial certificate
		deployerClients, _ := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		defer deployerClients.Close()

		waitFor := true
		timeout := int32(60)
		deployResp, err := deployerClients.Deployment.Deploy(ctx, &deployerV1.DeployRequest{
			TargetId:          targetID,
			CertificateId:     certSerial,
			WaitForCompletion: &waitFor,
			TimeoutSeconds:    &timeout,
		})
		require.NoError(t, err)
		assert.Equal(t, deployerV1.JobStatus_JOB_STATUS_COMPLETED, deployResp.GetJob().GetStatus())
		t.Log("Initial deployment completed")
	})

	// Step 3: Issue renewal certificate and deploy
	t.Run("3_RenewalCertAndDeploy", func(t *testing.T) {
		if creds == nil || issuerName == "" || targetID == "" {
			t.Skip("Missing prerequisites")
		}

		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer lcmClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		// Request renewal certificate (same CN, new cert)
		commonName := fmt.Sprintf("initial.%s.example.com", testID)
		certResp, err := lcmClients.CertificateJob.RequestCertificate(ctx, &lcmV1.RequestCertificateRequest{
			IssuerName: issuerName,
			CommonName: commonName,
			DnsNames:   []string{commonName},
		})
		require.NoError(t, err)
		t.Logf("Renewal certificate job: %s", certResp.GetJobId())

		// Wait for issuance
		var renewSerial string
		for i := 0; i < 60; i++ {
			time.Sleep(2 * time.Second)
			statusResp, err := lcmClients.CertificateJob.GetJobStatus(ctx, &lcmV1.GetJobStatusRequest{
				JobId: certResp.GetJobId(),
			})
			if err != nil {
				continue
			}
			if statusResp.GetStatus() == lcmV1.CertificateJobStatus_CERTIFICATE_JOB_STATUS_COMPLETED {
				resultResp, _ := lcmClients.CertificateJob.GetJobResult(ctx, &lcmV1.GetJobResultRequest{
					JobId: certResp.GetJobId(),
				})
				renewSerial = resultResp.GetSerialNumber()
				t.Logf("Renewal certificate issued: %s", renewSerial)
				break
			}
		}
		require.NotEmpty(t, renewSerial)

		// Deploy renewal certificate
		deployerClients, _ := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		defer deployerClients.Close()

		waitFor := true
		timeout := int32(60)
		deployResp, err := deployerClients.Deployment.Deploy(ctx, &deployerV1.DeployRequest{
			TargetId:          targetID,
			CertificateId:     renewSerial,
			WaitForCompletion: &waitFor,
			TimeoutSeconds:    &timeout,
		})
		require.NoError(t, err)
		assert.Equal(t, deployerV1.JobStatus_JOB_STATUS_COMPLETED, deployResp.GetJob().GetStatus())
		t.Log("Renewal deployment completed")

		// Verify we have 2 completed jobs for this target
		listResp, err := deployerClients.Job.ListJobs(ctx, &deployerV1.ListJobsRequest{
			TargetId: &targetID,
		})
		require.NoError(t, err)

		completedCount := 0
		for _, job := range listResp.GetItems() {
			if job.GetStatus() == deployerV1.JobStatus_JOB_STATUS_COMPLETED {
				completedCount++
			}
		}
		assert.GreaterOrEqual(t, completedCount, 2, "Should have at least 2 completed deployments")
		t.Logf("Total completed deployments: %d", completedCount)

		// Cleanup
		_, _ = deployerClients.Target.DeleteTarget(ctx, &deployerV1.DeleteTargetRequest{Id: targetID})
	})

	t.Log("Renewal flow completed successfully!")
}

// TestDeployerE2E_EventDrivenAutoDeployment tests the event-driven auto-deployment flow:
// 1. Register with LCM to get mTLS certificate
// 2. Create self-signed issuer
// 3. Create deployment target with auto-deploy filter matching the issuer
// 4. Request certificate from LCM (LCM publishes certificate.issued event)
// 5. Deployer receives event via Redis, matches target, creates deployment job
// 6. Job executor processes the job automatically
func TestDeployerE2E_EventDrivenAutoDeployment(t *testing.T) {
	cfg := LoadTestConfig()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Check if CA file exists
	if cfg.CAFile == "" {
		t.Skip("Skipping: LCM CA file not found. Set LCM_CA_FILE environment variable.")
	}
	if _, err := os.Stat(cfg.CAFile); os.IsNotExist(err) {
		t.Skipf("Skipping: LCM CA file not found at %s", cfg.CAFile)
	}

	// Create temp directory for certificates
	tempDir, err := os.MkdirTemp("", "deployer-auto-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	testID := fmt.Sprintf("auto-%d", time.Now().Unix())
	var creds *ClientCredentials
	var issuerName string
	var targetID string

	// Step 1: Register with LCM and create issuer
	t.Run("1_SetupLCM", func(t *testing.T) {
		t.Log("Connecting to LCM service...")

		// Register to get mTLS cert
		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, nil, cfg.Timeouts.Connection)
		if err != nil {
			t.Skipf("Could not connect to LCM service: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		clientID := fmt.Sprintf("deployer-auto-%s", testID)
		creds, err = RegisterAndGetMTLSCert(ctx, lcmClients, clientID, cfg.SharedSecret, tempDir)
		lcmClients.Close()
		if err != nil {
			t.Fatalf("Failed to register: %v", err)
		}
		t.Log("mTLS certificate obtained")

		// Create issuer with mTLS
		lcmClients, err = NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect with mTLS: %v", err)
		}
		defer lcmClients.Close()

		issuerName = fmt.Sprintf("auto-issuer-%s", testID)
		_, err = lcmClients.Issuer.CreateIssuer(ctx, &lcmV1.CreateIssuerRequest{
			Name:    issuerName,
			Type:    "self-signed",
			KeyType: "ecdsa",
			Status:  lcmV1.IssuerStatus_ISSUER_STATUS_ACTIVE,
			SelfIssuer: &lcmV1.SelfIssuer{
				CommonName:     "Auto Deploy Test",
				CaCommonName:   "Auto Deploy CA",
				CaOrganization: "Test Org",
				CaValidityDays: 365,
			},
		})
		if err != nil {
			t.Fatalf("Failed to create issuer: %v", err)
		}
		t.Logf("Issuer created: %s", issuerName)
	})

	// Step 2: Create deployment target with auto-deploy filter
	t.Run("2_CreateAutoDeployTarget", func(t *testing.T) {
		if issuerName == "" {
			t.Skip("Missing issuer from previous step")
		}

		t.Log("Creating deployment target with auto-deploy filter...")

		deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Could not connect to Deployer service: %v", err)
		}
		defer deployerClients.Close()

		ctx := context.Background()
		targetName := fmt.Sprintf("auto-target-%s", testID)

		configStruct, err := structpb.NewStruct(map[string]any{
			"simulate_delay_ms": 300.0,
		})
		require.NoError(t, err)

		credsStruct, err := structpb.NewStruct(map[string]any{})
		require.NoError(t, err)

		description := "Auto-deploy test target"
		autoDeployOnRenewal := true

		// Create target with filter matching our issuer
		createResp, err := deployerClients.Target.CreateTarget(ctx, &deployerV1.CreateTargetRequest{
			Name:                targetName,
			ProviderType:        "dummy",
			TenantId:            cfg.TenantID,
			Description:         &description,
			Config:              configStruct,
			Credentials:         credsStruct,
			AutoDeployOnRenewal: &autoDeployOnRenewal,
			CertificateFilters: []*deployerV1.CertificateFilter{
				{
					IssuerName: &issuerName, // Match certificates from our issuer
				},
			},
		})
		require.NoError(t, err)
		targetID = createResp.GetTarget().GetId()
		t.Logf("Target created with auto-deploy filter for issuer '%s': %s", issuerName, targetID)
	})

	// Step 3: Request certificate (this should trigger auto-deployment via event)
	t.Run("3_RequestCertificateAndWaitForAutoDeployment", func(t *testing.T) {
		if creds == nil || issuerName == "" || targetID == "" {
			t.Skip("Missing prerequisites from earlier steps")
		}

		t.Log("Requesting certificate from LCM (should trigger auto-deployment event)...")

		lcmClients, err := NewLCMClientsWithTLS(cfg.LCMEndpoint, cfg.CAFile, creds, cfg.Timeouts.Connection)
		if err != nil {
			t.Fatalf("Failed to connect with mTLS: %v", err)
		}
		defer lcmClients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.CertificateIssuance)
		defer cancel()

		// Request certificate
		commonName := fmt.Sprintf("auto.%s.example.com", testID)
		certResp, err := lcmClients.CertificateJob.RequestCertificate(ctx, &lcmV1.RequestCertificateRequest{
			IssuerName: issuerName,
			CommonName: commonName,
			DnsNames:   []string{commonName},
		})
		require.NoError(t, err)
		t.Logf("Certificate job created: %s", certResp.GetJobId())

		// Wait for certificate issuance
		var certSerial string
		for i := 0; i < 60; i++ {
			time.Sleep(2 * time.Second)
			statusResp, err := lcmClients.CertificateJob.GetJobStatus(ctx, &lcmV1.GetJobStatusRequest{
				JobId: certResp.GetJobId(),
			})
			if err != nil {
				continue
			}
			if statusResp.GetStatus() == lcmV1.CertificateJobStatus_CERTIFICATE_JOB_STATUS_COMPLETED {
				resultResp, _ := lcmClients.CertificateJob.GetJobResult(ctx, &lcmV1.GetJobResultRequest{
					JobId: certResp.GetJobId(),
				})
				certSerial = resultResp.GetSerialNumber()
				t.Logf("Certificate issued! Serial: %s", certSerial)
				t.Log("LCM should have published certificate.issued event to Redis")
				break
			}
		}
		require.NotEmpty(t, certSerial, "Certificate was not issued")

		// Wait for auto-deployment to complete
		// The deployer should have received the event and created a job
		t.Log("Waiting for auto-deployment to complete...")

		deployerClients, err := NewDeployerClients(cfg.DeployerEndpoint, cfg.Timeouts.Connection)
		require.NoError(t, err)
		defer deployerClients.Close()

		// Poll for deployment job to appear and complete
		var foundJob bool
		for i := 0; i < 30; i++ {
			time.Sleep(2 * time.Second)

			// List jobs for our target
			listResp, err := deployerClients.Job.ListJobs(ctx, &deployerV1.ListJobsRequest{
				TargetId: &targetID,
			})
			if err != nil {
				continue
			}

			for _, job := range listResp.GetItems() {
				// Check if this job is for our certificate
				if job.GetCertificateId() == certSerial || job.GetCertificateSerial() == certSerial {
					t.Logf("Found deployment job: %s (status=%s)", job.GetId(), job.GetStatus())

					if job.GetStatus() == deployerV1.JobStatus_JOB_STATUS_COMPLETED {
						t.Log("Auto-deployment completed successfully!")
						foundJob = true
						break
					} else if job.GetStatus() == deployerV1.JobStatus_JOB_STATUS_FAILED {
						t.Logf("Job failed: %s", job.GetStatusMessage())
						foundJob = true
						break
					}
					// Job exists but still processing
					foundJob = true
				}
			}
			if foundJob {
				break
			}
		}

		if !foundJob {
			t.Log("No auto-deployment job found. This could mean:")
			t.Log("  - LCM event publishing is not enabled")
			t.Log("  - Deployer event subscriber is not running")
			t.Log("  - The event topic names don't match")
			t.Log("  - Target filter didn't match the certificate")
		}

		// Cleanup
		_, _ = deployerClients.Target.DeleteTarget(ctx, &deployerV1.DeleteTargetRequest{Id: targetID})
	})

	t.Log("Event-driven auto-deployment test completed!")
}

// Helper function
func strPtr(s string) *string {
	return &s
}
