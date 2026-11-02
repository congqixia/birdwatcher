package states

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
)

func TestOSSPathCompletionValidation(t *testing.T) {
	validate := validator[configs.OSSPathCompletion]
	require.NotNil(t, validate)
	for _, value := range []string{"", "true", "false"} {
		require.NoError(t, validate(configs.OSSPathCompletion, value))
	}
	require.Error(t, validate(configs.OSSPathCompletion, "invalid"))
}
