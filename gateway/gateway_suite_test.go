package gateway

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

func TestGateway(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "Gateway Suite", []Reporter{testutils.NewJUnitReporter()})
}
