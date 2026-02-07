package e2e

import (
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// TestConfig holds configuration for deployer e2e tests
type TestConfig struct {
	LCMEndpoint      string
	DeployerEndpoint string
	RedisAddr        string
	RedisPassword    string
	CAFile           string
	SharedSecret     string
	TenantID         uint32
	Timeouts         TimeoutConfig
}

// TimeoutConfig holds timeout configuration
type TimeoutConfig struct {
	Connection          time.Duration
	CertificateIssuance time.Duration
	Deployment          time.Duration
	PollInterval        time.Duration
}

// LoadTestConfig loads test configuration from environment variables
func LoadTestConfig() *TestConfig {
	cfg := &TestConfig{
		LCMEndpoint:      getEnvOrDefault("LCM_GRPC_ENDPOINT", "localhost:9100"),
		DeployerEndpoint: getEnvOrDefault("DEPLOYER_GRPC_ENDPOINT", "localhost:9200"),
		RedisAddr:        getEnvOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    getEnvOrDefault("REDIS_PASSWORD", "*Abcd123456"),
		CAFile:           getEnvOrDefault("LCM_CA_FILE", findDefaultCAFile()),
		SharedSecret:     getEnvOrDefault("LCM_SHARED_SECRET", "changeme"),
		TenantID:         1,
		Timeouts: TimeoutConfig{
			Connection:          30 * time.Second,
			CertificateIssuance: 2 * time.Minute,
			Deployment:          1 * time.Minute,
			PollInterval:        2 * time.Second,
		},
	}
	return cfg
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// findDefaultCAFile tries to find the LCM CA file
func findDefaultCAFile() string {
	// Try common locations
	candidates := []string{
		// Relative to test directory
		"../../../../../../app/lcm/service/data/ca/ca.crt",
		"../../../lcm/service/data/ca/ca.crt",
		// Absolute paths
		"/home/sko/projects/go-tangra/go-tangra-portal/app/lcm/service/data/ca/ca.crt",
	}

	// Also try relative to current file
	_, filename, _, ok := runtime.Caller(0)
	if ok {
		testDir := filepath.Dir(filename)
		candidates = append(candidates,
			filepath.Join(testDir, "../../../../../../app/lcm/service/data/ca/ca.crt"),
			filepath.Join(testDir, "../../../../../app/lcm/service/data/ca/ca.crt"),
		)
	}

	for _, path := range candidates {
		absPath, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if _, err := os.Stat(absPath); err == nil {
			return absPath
		}
	}

	return ""
}
