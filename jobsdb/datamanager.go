package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/distributed"
	uuid "github.com/satori/go.uuid"
)

type CustomerQueue struct {
	GatewayJobsdb      *HandleT
	RouterJobsdb       *HandleT
	BatchrouterJobsddb *HandleT
	ProcerrorJobsdb    *HandleT
}

var customerQueues map[string]*CustomerQueue

func SetupCustomerQueues(clearAll bool) {
	customerQueues = make(map[string]*CustomerQueue)
	customers := distributed.GetCustomerList()
	psqlInfo := GetConnectionString()
	dbHandle, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	dbHandle.SetMaxOpenConns(64)

	for _, customer := range customers {
		var gatewayDB HandleT
		var routerDB HandleT
		var batchRouterDB HandleT
		var procErrorDB HandleT

		migrateDBHandle, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		//TODO: fix values passed
		gatewayDB.Setup(migrateDBHandle, dbHandle, dbHandle, ReadWrite, clearAll, customer.Name+"_"+"gw", time.Hour*10000, "", true, QueryFiltersT{})
		//setting up router, batch router, proc error DBs also irrespective of server mode
		migrateDBHandle, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		routerDB.Setup(migrateDBHandle, dbHandle, dbHandle, ReadWrite, clearAll, customer.Name+"_"+"rt", time.Hour*10000, "", true, QueryFiltersT{})
		migrateDBHandle, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		batchRouterDB.Setup(migrateDBHandle, dbHandle, dbHandle, ReadWrite, clearAll, customer.Name+"_"+"batch_rt", time.Hour*10000, "", true, QueryFiltersT{})
		migrateDBHandle, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		procErrorDB.Setup(migrateDBHandle, dbHandle, dbHandle, ReadWrite, clearAll, customer.Name+"_"+"proc_error", time.Hour*10000, "", false, QueryFiltersT{})

		customerQueues[customer.WorkspaceID] = &CustomerQueue{
			GatewayJobsdb:      &gatewayDB,
			RouterJobsdb:       &routerDB,
			BatchrouterJobsddb: &batchRouterDB,
			ProcerrorJobsdb:    &procErrorDB,
		}
	}
}

func getQueueForCustomer(customerWorkspaceID, queue string) *HandleT {
	customerQueue := customerQueues[customerWorkspaceID]
	switch queue {
	case "gw":
		return customerQueue.GatewayJobsdb
	case "rt":
		return customerQueue.RouterJobsdb
	case "batch_rt":
		return customerQueue.BatchrouterJobsddb
	case "proc_error":
		return customerQueue.ProcerrorJobsdb
	}

	panic("Unknow queue")
}

func Store(jobList []*JobT, queue string) error {
	//TODO remove loop on jobList again for performance benefits
	//TODO handle errors properly
	customerJobListMap := make(map[string][]*JobT)
	for _, job := range jobList {
		if _, ok := customerJobListMap[job.Customer]; !ok {
			customerJobListMap[job.Customer] = make([]*JobT, 0)
		}
		customerJobListMap[job.Customer] = append(customerJobListMap[job.Customer], job)
	}

	for customer, list := range customerJobListMap {
		StoreJobsForCustomer(customer, queue, list)
	}
	return nil
}

func StoreJobsForCustomer(customer string, queue string, list []*JobT) error {
	getQueueForCustomer(customer, queue).Store(list)
	return nil
}

func StoreWithRetryEach(jobList []*JobT, queue string) map[uuid.UUID]string {
	//TODO remove loop on jobList again for performance benefits
	//TODO handle errors properly
	customerJobListMap := make(map[string][]*JobT)
	for _, job := range jobList {
		if _, ok := customerJobListMap[job.Customer]; !ok {
			customerJobListMap[job.Customer] = make([]*JobT, 0)
		}
		customerJobListMap[job.Customer] = append(customerJobListMap[job.Customer], job)
	}

	maps := make([]map[uuid.UUID]string, 0)
	for customer, list := range customerJobListMap {
		maps = append(maps, getQueueForCustomer(customer, queue).StoreWithRetryEach(list))
	}
	return MergeMaps(maps...)
}

func UpdateJobStatus(jobStatusList []*JobStatusT, customValFilers []string, parameterFilters []ParameterFilterT, customer string, queueType string) error {
	err := getQueueForCustomer(customer, queueType).UpdateJobStatus(jobStatusList, customValFilers, parameterFilters)
	return err
}

func DeleteExecuting(params GetQueryParamsT, queueType string) {
	for customer := range customerQueues {
		getQueueForCustomer(customer, queueType).DeleteExecuting(params)
	}
}

func BeginGlobalTransaction(customer string, queueType string) *sql.Tx {
	return getQueueForCustomer(customer, queueType).BeginGlobalTransaction()
}

func AcquireUpdateJobStatusLocks(customer string, queueType string) {
	getQueueForCustomer(customer, queueType).AcquireUpdateJobStatusLocks()
}

func UpdateJobStatusInTxn(txn *sql.Tx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT, customer string, queueType string) error {
	err := getQueueForCustomer(customer, queueType).UpdateJobStatusInTxn(txn, statusList, customValFilters, parameterFilters)
	return err

}

func CommitTransaction(txn *sql.Tx, customer string, queueType string) {
	getQueueForCustomer(customer, queueType).CommitTransaction(txn)
}

func ReleaseUpdateJobStatusLocks(customer string, queueType string) {
	getQueueForCustomer(customer, queueType).ReleaseUpdateJobStatusLocks()
}

func GetToRetry(params GetQueryParamsT, customer string, queueType string) []*JobT {
	return getQueueForCustomer(customer, queueType).GetToRetry(params)
}

func GetThrottled(params GetQueryParamsT, customer string, queueType string) []*JobT {
	return getQueueForCustomer(customer, queueType).GetThrottled(params)
}

func GetWaiting(params GetQueryParamsT, customer string, queueType string) []*JobT {
	return getQueueForCustomer(customer, queueType).GetWaiting(params)
}

func GetUnprocessed(params GetQueryParamsT, customer string, queueType string) []*JobT {
	return getQueueForCustomer(customer, queueType).GetUnprocessed(params)
}

func MergeMaps(maps ...map[uuid.UUID]string) (result map[uuid.UUID]string) {
	result = make(map[uuid.UUID]string)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func JournalDeleteEntry(customer, queue string, opID int64) {
	getQueueForCustomer(customer, queue).JournalDeleteEntry(opID)
}

func JournalMarkStart(customer, queue string, operation string, opPayload json.RawMessage) int64 {
	return getQueueForCustomer(customer, queue).JournalMarkStart(operation, opPayload)
}

func GetJournalEntries(opType, customer, queue string) (entries []JournalEntryT) {
	return getQueueForCustomer(customer, queue).GetJournalEntries(opType)
}

func GetImportingList(params GetQueryParamsT, customer, queue string) []*JobT {
	return getQueueForCustomer(customer, queue).GetImportingList(params)
}

func GetUnionUnprocessed(params GetQueryParamsT, queueType string) []*JobT {
	toQuery := params.Count
	customValFilters := params.CustomValFilters
	// parameterFilters := params.ParameterFilters
	//create query here
	configList := distributed.GetAllCustomersComputeConfig()
	unionQueries := make([]string, 0)
	for customer, config := range configList {
		count := int(config.ComputeShare * float32(toQuery))
		jd := getQueueForCustomer(customer, queueType)
		jd.dsMigrationLock.RLock()
		jd.dsListLock.RLock()
		defer jd.dsMigrationLock.RUnlock()
		defer jd.dsListLock.RUnlock()

		ds := jd.getDSList(false)[0]
		sqlStatement := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val,
                                               %[1]s.event_payload, %[1]s.created_at,
                                               %[1]s.expire_at
                                             FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id
                                             WHERE %[2]s.job_id is NULL`, ds.JobTable, ds.JobStatusTable)
		if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
			sqlStatement += " AND " + constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
		}
		sqlStatement += fmt.Sprintf(" ORDER BY %s.job_id", ds.JobTable)
		sqlStatement += fmt.Sprintf(" LIMIT %d", count)
		unionQueries = append(unionQueries, sqlStatement)
	}

	unionQuery := strings.Join(unionQueries, ") UNION (")
	finalQuery := "(" + unionQuery + ")"
	// pkgLogger.Info(finalQuery)

	jd := getQueueForCustomer(distributed.GetCustomerList()[0].WorkspaceID, queueType)
	rows, err := jd.dbHandle.Query(finalQuery)
	jd.assertError(err)
	defer rows.Close()

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt)
		jd.assertError(err)
		jobList = append(jobList, &job)
	}
	// pkgLogger.Info(jobList)

	return jobList
}

func GetUnionProcessed(params GetQueryParamsT, queueType string) []*JobT {
	toQuery := params.Count
	customValFilters := params.CustomValFilters
	stateFilters := params.StateFilters
	// parameterFilters := params.ParameterFilters
	//create query here

	configList := distributed.GetAllCustomersComputeConfig()
	unionQueries := make([]string, 0)
	for customer, config := range configList {
		count := int(config.ComputeShare * float32(toQuery))
		jd := getQueueForCustomer(customer, queueType)
		jd.dsMigrationLock.RLock()
		jd.dsListLock.RLock()
		defer jd.dsMigrationLock.RUnlock()
		defer jd.dsListLock.RUnlock()

		ds := jd.getDSList(false)[0]
		stateQuery := " AND " + constructQuery(jd, "job_state", stateFilters, "OR")
		customValQuery := " AND " +
			constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
		sourceQuery := ""
		limitQuery := fmt.Sprintf(" LIMIT %d ", count)
		sqlStatement := fmt.Sprintf(`SELECT
				%[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload,
				%[1]s.created_at, %[1]s.expire_at,
				job_latest_state.job_state, job_latest_state.attempt,
				job_latest_state.exec_time, job_latest_state.retry_time,
				job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters
			 FROM
				%[1]s,
				(SELECT job_id, job_state, attempt, exec_time, retry_time,
				  error_code, error_response, parameters FROM %[2]s WHERE id IN
					(SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
				AS job_latest_state
			 WHERE %[1]s.job_id=job_latest_state.job_id
			  %[4]s %[5]s
			  AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
			ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)
		unionQueries = append(unionQueries, sqlStatement)
	}
	unionQuery := strings.Join(unionQueries, ") UNION (")
	finalQuery := "(" + unionQuery + ")"
	// pkgLogger.Info(finalQuery)

	jd := getQueueForCustomer(distributed.GetCustomerList()[0].WorkspaceID, queueType)
	stmt, err := jd.dbHandle.Prepare(finalQuery)
	jd.assertError(err)
	defer stmt.Close()
	rows, err := stmt.Query(getTimeNowFunc())
	jd.assertError(err)
	defer rows.Close()

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
			&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
			&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
			&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse, &job.LastJobStatus.Parameters)
		jd.assertError(err)
		jobList = append(jobList, &job)
	}

	return jobList
}
