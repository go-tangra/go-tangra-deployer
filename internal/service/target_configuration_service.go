package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"io"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/go-tangra/go-tangra-deployer/internal/conf"
	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/targetconfiguration"
	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

// TargetConfigurationService implements the TargetConfigurationService gRPC service
type TargetConfigurationService struct {
	deployerV1.UnimplementedTargetConfigurationServiceServer

	log           *log.Helper
	configRepo    *data.TargetConfigurationRepo
	encryptionKey []byte
}

// NewTargetConfigurationService creates a new TargetConfigurationService
func NewTargetConfigurationService(
	ctx *bootstrap.Context,
	configRepo *data.TargetConfigurationRepo,
) *TargetConfigurationService {
	// Get encryption key from config
	var encryptionKey []byte
	if cfg, ok := ctx.GetCustomConfig("deployer"); ok && cfg != nil {
		if deployerCfg, ok := cfg.(*conf.Deployer); ok && deployerCfg.Encryption != nil {
			encryptionKey = []byte(deployerCfg.Encryption.Key)
		}
	}

	// Ensure key is 32 bytes for AES-256
	if len(encryptionKey) < 32 {
		padded := make([]byte, 32)
		copy(padded, encryptionKey)
		encryptionKey = padded
	} else if len(encryptionKey) > 32 {
		encryptionKey = encryptionKey[:32]
	}

	return &TargetConfigurationService{
		log:           ctx.NewLoggerHelper("deployer/service/target-configuration"),
		configRepo:    configRepo,
		encryptionKey: encryptionKey,
	}
}

// CreateConfiguration creates a new target configuration
func (s *TargetConfigurationService) CreateConfiguration(ctx context.Context, req *deployerV1.CreateConfigurationRequest) (*deployerV1.CreateConfigurationResponse, error) {
	s.log.Infof("CreateConfiguration: tenant_id=%d, name=%s, provider=%s", req.GetTenantId(), req.GetName(), req.GetProviderType())

	// Validate provider exists
	if !registry.Exists(req.GetProviderType()) {
		return nil, deployerV1.ErrorProviderNotFound("provider type '%s' not found", req.GetProviderType())
	}

	// Check for duplicate name
	existing, err := s.configRepo.GetByTenantAndName(ctx, req.GetTenantId(), req.GetName())
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, deployerV1.ErrorConfigurationNameExists("configuration with name '%s' already exists", req.GetName())
	}

	// Convert config
	config := structToMap(req.GetConfig())

	// Validate and encrypt credentials
	credentials := structToMap(req.GetCredentials())
	provider, err := registry.Get(req.GetProviderType())
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, deployerV1.ErrorCredentialsInvalid("credentials validation failed: %v", err)
	}

	encryptedCreds, err := s.encryptCredentials(credentials)
	if err != nil {
		return nil, deployerV1.ErrorInternalServerError("failed to encrypt credentials")
	}

	// Get description
	var description string
	if req.Description != nil {
		description = *req.Description
	}

	entity, err := s.configRepo.Create(ctx, req.GetTenantId(), req.GetName(), description,
		req.GetProviderType(), encryptedCreds, config)
	if err != nil {
		return nil, err
	}

	return &deployerV1.CreateConfigurationResponse{
		Configuration: s.configRepo.ToProto(entity),
	}, nil
}

// GetConfiguration gets a target configuration by ID
func (s *TargetConfigurationService) GetConfiguration(ctx context.Context, req *deployerV1.GetConfigurationRequest) (*deployerV1.GetConfigurationResponse, error) {
	s.log.Infof("GetConfiguration: id=%s", req.GetId())

	entity, err := s.configRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
	}

	return &deployerV1.GetConfigurationResponse{
		Configuration: s.configRepo.ToProto(entity),
	}, nil
}

// ListConfigurations lists target configurations
func (s *TargetConfigurationService) ListConfigurations(ctx context.Context, req *deployerV1.ListConfigurationsRequest) (*deployerV1.ListConfigurationsResponse, error) {
	s.log.Infof("ListConfigurations: tenant_id=%v", req.TenantId)

	// Convert status
	var status *targetconfiguration.Status
	if req.Status != nil {
		var st targetconfiguration.Status
		switch *req.Status {
		case deployerV1.ConfigurationStatus_CONFIG_STATUS_ACTIVE:
			st = targetconfiguration.StatusCONFIG_STATUS_ACTIVE
		case deployerV1.ConfigurationStatus_CONFIG_STATUS_INACTIVE:
			st = targetconfiguration.StatusCONFIG_STATUS_INACTIVE
		case deployerV1.ConfigurationStatus_CONFIG_STATUS_ERROR:
			st = targetconfiguration.StatusCONFIG_STATUS_ERROR
		}
		status = &st
	}

	page := uint32(1)
	pageSize := uint32(20)
	if req.Page != nil && *req.Page > 0 {
		page = *req.Page
	}
	if req.PageSize != nil && *req.PageSize > 0 {
		pageSize = *req.PageSize
	}

	entities, total, err := s.configRepo.List(ctx, req.TenantId, req.ProviderType, status, page, pageSize)
	if err != nil {
		return nil, err
	}

	items := make([]*deployerV1.TargetConfiguration, 0, len(entities))
	for _, entity := range entities {
		items = append(items, s.configRepo.ToProto(entity))
	}

	return &deployerV1.ListConfigurationsResponse{
		Items: items,
		Total: uint64(total),
	}, nil
}

// UpdateConfiguration updates a target configuration
func (s *TargetConfigurationService) UpdateConfiguration(ctx context.Context, req *deployerV1.UpdateConfigurationRequest) (*deployerV1.UpdateConfigurationResponse, error) {
	s.log.Infof("UpdateConfiguration: id=%s", req.GetId())

	// Validate configuration exists
	existing, err := s.configRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
	}

	// Convert config
	var config map[string]any
	if req.Config != nil {
		config = structToMap(req.Config)
	} else if existing.Config != nil {
		config = existing.Config
	}

	// Handle credentials update
	var encryptedCreds []byte
	if req.Credentials != nil {
		credentials := structToMap(req.Credentials)
		provider, err := registry.Get(existing.ProviderType)
		if err != nil {
			return nil, err
		}
		if err := provider.ValidateCredentials(ctx, credentials, config); err != nil {
			return nil, deployerV1.ErrorCredentialsInvalid("credentials validation failed: %v", err)
		}
		encryptedCreds, err = s.encryptCredentials(credentials)
		if err != nil {
			return nil, deployerV1.ErrorInternalServerError("failed to encrypt credentials")
		}
	}

	// Convert status
	var status *targetconfiguration.Status
	if req.Status != nil {
		var st targetconfiguration.Status
		switch *req.Status {
		case deployerV1.ConfigurationStatus_CONFIG_STATUS_ACTIVE:
			st = targetconfiguration.StatusCONFIG_STATUS_ACTIVE
		case deployerV1.ConfigurationStatus_CONFIG_STATUS_INACTIVE:
			st = targetconfiguration.StatusCONFIG_STATUS_INACTIVE
		case deployerV1.ConfigurationStatus_CONFIG_STATUS_ERROR:
			st = targetconfiguration.StatusCONFIG_STATUS_ERROR
		}
		status = &st
	}

	entity, err := s.configRepo.Update(ctx, req.GetId(), req.Name, req.Description,
		encryptedCreds, config, status)
	if err != nil {
		return nil, err
	}

	return &deployerV1.UpdateConfigurationResponse{
		Configuration: s.configRepo.ToProto(entity),
	}, nil
}

// DeleteConfiguration deletes a target configuration
func (s *TargetConfigurationService) DeleteConfiguration(ctx context.Context, req *deployerV1.DeleteConfigurationRequest) (*emptypb.Empty, error) {
	s.log.Infof("DeleteConfiguration: id=%s", req.GetId())

	if err := s.configRepo.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ValidateCredentials validates provider credentials
func (s *TargetConfigurationService) ValidateCredentials(ctx context.Context, req *deployerV1.ValidateConfigurationCredentialsRequest) (*deployerV1.ValidateConfigurationCredentialsResponse, error) {
	s.log.Infof("ValidateCredentials: provider=%s", req.GetProviderType())

	provider, err := registry.Get(req.GetProviderType())
	if err != nil {
		return &deployerV1.ValidateConfigurationCredentialsResponse{
			Valid:   false,
			Message: stringPtr("provider not found"),
		}, nil
	}

	credentials := structToMap(req.GetCredentials())
	config := structToMap(req.GetConfig())
	if err := provider.ValidateCredentials(ctx, credentials, config); err != nil {
		return &deployerV1.ValidateConfigurationCredentialsResponse{
			Valid:   false,
			Message: stringPtr(err.Error()),
		}, nil
	}

	return &deployerV1.ValidateConfigurationCredentialsResponse{
		Valid:   true,
		Message: stringPtr("credentials are valid"),
	}, nil
}

// ListProviders lists available providers
func (s *TargetConfigurationService) ListProviders(ctx context.Context, req *deployerV1.ListConfigurationProvidersRequest) (*deployerV1.ListConfigurationProvidersResponse, error) {
	s.log.Info("ListProviders")

	infos := registry.List()
	providers := make([]*deployerV1.ProviderInfo, 0, len(infos))

	for _, info := range infos {
		providers = append(providers, &deployerV1.ProviderInfo{
			Type:                     info.Type,
			DisplayName:              info.DisplayName,
			Description:              info.Description,
			SupportsVerification:     info.Caps.SupportsVerification,
			SupportsRollback:         info.Caps.SupportsRollback,
			RequiredConfigFields:     info.Caps.RequiredConfigFields,
			RequiredCredentialFields: info.Caps.RequiredCredFields,
		})
	}

	return &deployerV1.ListConfigurationProvidersResponse{
		Providers: providers,
	}, nil
}

// GetDecryptedCredentials gets the decrypted credentials for a configuration (internal use)
func (s *TargetConfigurationService) GetDecryptedCredentials(ctx context.Context, configID string) (map[string]any, error) {
	encryptedCreds, err := s.configRepo.GetCredentialsEncrypted(ctx, configID)
	if err != nil {
		return nil, err
	}
	return s.decryptCredentials(encryptedCreds)
}

// encryptCredentials encrypts credentials using AES-GCM
func (s *TargetConfigurationService) encryptCredentials(credentials map[string]any) ([]byte, error) {
	plaintext, err := json.Marshal(credentials)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(s.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decryptCredentials decrypts credentials using AES-GCM
func (s *TargetConfigurationService) decryptCredentials(ciphertext []byte) (map[string]any, error) {
	block, err := aes.NewCipher(s.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, deployerV1.ErrorInternalServerError("invalid ciphertext")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	var credentials map[string]any
	if err := json.Unmarshal(plaintext, &credentials); err != nil {
		return nil, err
	}

	return credentials, nil
}

// structToMap converts a protobuf Struct to a Go map
func structToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

// stringPtr returns a pointer to a string, or nil if empty
func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
