package app

//go:generate mockgen -destination=../mocks/app/mock_features.go -package=mock_app github.com/rudderlabs/rudder-server/app MigratorFeature,SuppressUserFeature

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// MigratorFeature handles migration of nodes during cluster's scale up/down.
type MigratorFeature interface {
	Run(context.Context, *jobsdb.HandleT, *jobsdb.HandleT, *jobsdb.HandleT, func(), func())
	PrepareJobsdbsForImport(*jobsdb.HandleT, *jobsdb.HandleT, *jobsdb.HandleT)
}

// MigratorFeatureSetup is a function that initializes a Migrator feature, based on application instance
type MigratorFeatureSetup func(Interface) MigratorFeature

var migratorFeatureSetup MigratorFeatureSetup

// RegisterMigratorFeature registers a Migration implementation
func RegisterMigratorFeature(f MigratorFeatureSetup) {
	migratorFeatureSetup = f
}

// SuppressUserFeature handles webhook event requests
type SuppressUserFeature interface {
	Setup(backendConfig backendconfig.BackendConfig) types.SuppressUserI
}

// SuppressUserFeatureSetup is a function that initializes a Webhook feature, based on application instance
type SuppressUserFeatureSetup func(Interface) SuppressUserFeature

var suppressUserFeatureSetup SuppressUserFeatureSetup

// RegisterSuppressUserFeature registers a suppress user feature implementation
func RegisterSuppressUserFeature(f SuppressUserFeatureSetup) {
	suppressUserFeatureSetup = f
}

/*********************************
DestinationConfig Env Support
*********************************/

// ConfigEnvFeature handles override of config from ENV variables.
type ConfigEnvFeature interface {
	Setup() types.ConfigEnvI
}

// ConfigEnvFeatureSetup is a function that initializes a ConfigEnv feature
type ConfigEnvFeatureSetup func(Interface) ConfigEnvFeature

var configEnvFeatureSetup ConfigEnvFeatureSetup

// RegisterConfigEnvFeature registers a config env feature implementation
func RegisterConfigEnvFeature(f ConfigEnvFeatureSetup) {
	configEnvFeatureSetup = f
}

/*********************************
Reporting Feature
*********************************/

// ReportingFeature handles reporting statuses / errors to reporting service
type ReportingFeature interface {
	Setup(backendConfig backendconfig.BackendConfig) types.ReportingI
	GetReportingInstance() types.ReportingI
}

// ReportingFeatureSetup is a function that initializes a Reporting feature
type ReportingFeatureSetup func(Interface) ReportingFeature

var reportingFeatureSetup ReportingFeatureSetup

// RegisterReportingFeature registers a config env feature implementation
func RegisterReportingFeature(f ReportingFeatureSetup) {
	reportingFeatureSetup = f
}

/*********************************
Replay Feature
*********************************/

// ReplayFeature handles inserting of failed jobs into repsective gw/rt jobsdb
type ReplayFeature interface {
	Setup(replayDB *jobsdb.HandleT, gwDB *jobsdb.HandleT, routerDB *jobsdb.HandleT)
}

// ReplayFeatureSetup is a function that initializes a Replay feature
type ReplayFeatureSetup func(Interface) ReplayFeature

var replayFeatureSetup ReplayFeatureSetup

// RegisterReplayFeature registers a config env feature implementation
func RegisterReplayFeature(f ReplayFeatureSetup) {
	replayFeatureSetup = f
}

// Features contains optional implementations of Enterprise only features.
type Features struct {
	Migrator     MigratorFeature
	SuppressUser SuppressUserFeature
	ConfigEnv    ConfigEnvFeature
	Reporting    ReportingFeature
	Replay       ReplayFeature
}
