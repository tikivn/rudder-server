package multitenant

import (
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

var NOOP = noop{}

type noop struct{}

func (*noop) CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool) {
}

func (*noop) GenerateSuccessRateMap(destType string) (map[string]float64, map[string]float64) {
	return nil, nil
}

func (*noop) AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
}

func (*noop) GetRouterPickupJobs(destType string, recentJobInResultSet map[string]time.Time, sortedLatencyList []string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, successRateMap map[string]float64, drainedMap map[string]float64, timeGained float64) (map[string]int, map[string]float64) {
	return nil, nil
}

func (*noop) RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	return
}

func (*noop) ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
	return
}