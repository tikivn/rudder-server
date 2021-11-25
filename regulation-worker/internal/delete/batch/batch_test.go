package batch_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	config.Load()
	logger.Init()

	ctx := context.Background()
	tests := []struct {
		name           string
		job            model.Job
		dest           model.Destination
		expectedErr    error
		expectedStatus model.JobStatus
	}{
		{
			name: "testing batch deletion flow by deletion from 'mock_batch' destination",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatusPending,
				UserAttributes: []model.UserAttribute{
					{
						UserID: "Jermaine1473336609491897794707338",
						Phone:  strPtr("6463633841"),
						Email:  strPtr("dorowane8n285680461479465450293436@gmail.com"),
					},
					{
						UserID: "Mercie8221821544021583104106123",
						Email:  strPtr("dshirilad8536019424659691213279980@gmail.com"),
					},
					{
						UserID: "Claiborn443446989226249191822329",
						Phone:  strPtr("8782905113"),
					},
				},
			},
			dest: model.Destination{
				Config: map[string]interface{}{
					"bucketName":  "regulation-test-data",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
					"prefix":      "reg-original",
				},
				Name: "S3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := batch.Delete(ctx, tt.job, tt.dest.Config, tt.dest.Name)
			require.NoError(t, err, "expected no error")
		})

	}
}

func strPtr(str string) *string {
	return &(str)
}
