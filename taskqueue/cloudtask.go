package taskqueue

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/internal"
	pb "google.golang.org/appengine/internal/taskqueue"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	cloudtasksbeta "cloud.google.com/go/cloudtasks/apiv2beta3"
	taskspbbeta "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
)

const (
	maxTaskPayloadBytes   = 100 * 1024 // 100 KB max payload size for Cloud Tasks
	maxTransactionalTasks = 5          // Maximum tasks allowed in a single Datastore transaction
	batchCreateChunkSize  = 100        // Maximum tasks per BatchCreateTasks request
	batchDeleteChunkSize  = 1000       // Maximum tasks per BatchDeleteTasks request

	grpcNotFound      = 5
	grpcAlreadyExists = 6
	httpNotFound      = 404
	httpAlreadyExists = 409
)

var (
	taskNameRegex                = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	metadataRegionURL            = "http://metadata.google.internal/computeMetadata/v1/instance/region"
	cloudTasksClientOpts         []option.ClientOption
	ErrTooManyTasksInTransaction = &internal.APIError{
		Service: "taskqueue",
		Detail:  "too many tasks in transaction",
		Code:    int32(pb.TaskQueueServiceError_TOO_MANY_TASKS),
	}
)

var (
	// Cloud Tasks gRPC clients are expensive to create (credentials lookup,
	// connection setup), so they are created lazily once and reused for the
	// lifetime of the process. They are safe for concurrent use.
	cloudTasksClientMu   sync.Mutex
	cloudTasksClient     *cloudtasks.Client
	cloudTasksBetaClient *cloudtasksbeta.Client
)

// getCloudTasksClient returns the shared Cloud Tasks v2 client, creating it on
// first use. A failed creation is not cached, so the next call retries.
func getCloudTasksClient() (*cloudtasks.Client, error) {
	cloudTasksClientMu.Lock()
	defer cloudTasksClientMu.Unlock()
	if cloudTasksClient == nil {
		// context.Background is used because the client outlives the request.
		c, err := cloudtasks.NewClient(context.Background(), cloudTasksClientOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create cloudtasks client: %v", err)
		}
		cloudTasksClient = c
	}
	return cloudTasksClient, nil
}

// getCloudTasksBetaClient returns the shared Cloud Tasks v2beta3 client, used
// only for QueueStats, creating it on first use.
func getCloudTasksBetaClient() (*cloudtasksbeta.Client, error) {
	cloudTasksClientMu.Lock()
	defer cloudTasksClientMu.Unlock()
	if cloudTasksBetaClient == nil {
		c, err := cloudtasksbeta.NewClient(context.Background(), cloudTasksClientOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create cloudtasks v2beta3 client: %v", err)
		}
		cloudTasksBetaClient = c
	}
	return cloudTasksBetaClient, nil
}

func useCloudTasks() bool {
	v, _ := strconv.ParseBool(os.Getenv("APPENGINE_USE_CLOUDTASK_PUSH_QUEUE"))
	return v
}

func newUnknownTaskError(detail string) error {
	return &internal.APIError{
		Service: "taskqueue",
		Detail:  detail,
		Code:    int32(pb.TaskQueueServiceError_UNKNOWN_TASK),
	}
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "AlreadyExists") || strings.Contains(msg, "already exists") || strings.Contains(msg, "409") || strings.Contains(msg, "Policy checks are unavailable")
}

func isUnimplementedError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Unimplemented") || strings.Contains(msg, "unknown method") || strings.Contains(msg, "404")
}

func getQueuePath(ctx context.Context, queueName string) (string, error) {
	if queueName == "" {
		queueName = "default"
	}
	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}
	region, err := getRegion(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get region: %v", err)
	}
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s", project, region, queueName), nil
}

func getRegion(ctx context.Context) (string, error) {
	req, err := http.NewRequest("GET", metadataRegionURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	parts := strings.Split(strings.TrimSpace(string(body)), "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("invalid region format: %s", string(body))
	}
	return parts[len(parts)-1], nil
}

func sendTask(ctx context.Context, queueName string, taskName string, taskObj *taskspb.Task) (string, error) {
	parent, err := getQueuePath(ctx, queueName)
	if err != nil {
		return "", err
	}

	client, err := getCloudTasksClient()
	if err != nil {
		return "", err
	}

	req := &taskspb.CreateTaskRequest{
		Parent: parent,
		Task:   taskObj,
	}

	createdTask, err := client.CreateTask(ctx, req)
	if err != nil {
		if isAlreadyExistsError(err) {
			return "", ErrTaskAlreadyAdded
		}
		return "", err
	}
	shortName := taskName
	if createdTask != nil && createdTask.Name != "" {
		if idx := strings.LastIndex(createdTask.Name, "/"); idx != -1 {
			shortName = createdTask.Name[idx+1:]
		} else {
			shortName = createdTask.Name
		}
	}
	return shortName, nil
}

func extractServiceFromHost(ctx context.Context, host string) string {
	if host == "" {
		if s := os.Getenv("GAE_SERVICE"); s != "" {
			return s
		}
		return "default"
	}

	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}

	pIdx := strings.Index(host, project)
	if pIdx == -1 {
		defaultHost := appengine.DefaultVersionHostname(ctx)
		if host == defaultHost {
			return "default"
		}
		return host
	}

	domainSuffix := host[pIdx:]
	if host == domainSuffix {
		return "default"
	}

	suffixes := []string{
		"." + domainSuffix,
		"-dot-" + domainSuffix,
	}
	stripped := host
	for _, suffix := range suffixes {
		if strings.HasSuffix(stripped, suffix) {
			stripped = stripped[:len(stripped)-len(suffix)]
			break
		}
	}

	if stripped == host {
		return host
	}

	stripped = strings.ReplaceAll(stripped, "-dot-", ".")
	parts := strings.Split(stripped, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return "default"
}

func buildCloudTaskProto(ctx context.Context, queueName string, task *Task) (*taskspb.Task, string, error) {
	if task.Name != "" {
		if !taskNameRegex.MatchString(task.Name) {
			return nil, "", fmt.Errorf("taskqueue: invalid task name %q", task.Name)
		}
	}

	if len(task.Payload) > maxTaskPayloadBytes {
		return nil, "", fmt.Errorf("taskqueue: task too large (%d bytes)", len(task.Payload))
	}

	queuePath, err := getQueuePath(ctx, queueName)
	if err != nil {
		return nil, "", err
	}

	taskName := task.Name
	var fullTaskName string
	if taskName != "" {
		fullTaskName = fmt.Sprintf("%s/tasks/%s", queuePath, taskName)
	}

	path := task.Path
	if path == "" {
		path = "/_ah/queue/" + queueName
	}

	headers := make(map[string]string)
	for k, vs := range task.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	if _, ok := headers["Content-Type"]; !ok {
		headers["Content-Type"] = "application/octet-stream"
	}
	if _, ok := headers["X-AppEngine-QueueName"]; !ok {
		headers["X-AppEngine-QueueName"] = queueName
	}
	if taskName != "" {
		if _, ok := headers["X-AppEngine-TaskName"]; !ok {
			headers["X-AppEngine-TaskName"] = taskName
		}
	}

	targetService := extractServiceFromHost(ctx, headers["Host"])
	var routing *taskspb.AppEngineRouting
	if targetService != "" {
		routing = &taskspb.AppEngineRouting{
			Service: targetService,
		}
	}
	delete(headers, "Host")
	ae := &taskspb.AppEngineHttpRequest{
		RelativeUri:      path,
		Headers:          headers,
		Body:             task.Payload,
		AppEngineRouting: routing,
	}
	if code, ok := taskspb.HttpMethod_value[task.method()]; ok {
		ae.HttpMethod = taskspb.HttpMethod(code)
	}

	taskObj := &taskspb.Task{
		Name: fullTaskName,
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: ae,
		},
	}

	if !task.ETA.IsZero() {
		taskObj.ScheduleTime = timestamppb.New(task.ETA)
	} else if task.Delay > 0 {
		taskObj.ScheduleTime = timestamppb.New(time.Now().Add(task.Delay))
	}

	if task.RetryOptions != nil {
		rc := &taskspb.RetryConfig{}
		hasRC := false
		if task.RetryOptions.RetryLimit > 0 {
			rc.MaxAttempts = task.RetryOptions.RetryLimit
			hasRC = true
		}
		if task.RetryOptions.AgeLimit > 0 {
			rc.MaxRetryDuration = durationpb.New(task.RetryOptions.AgeLimit)
			hasRC = true
		}
		if task.RetryOptions.MinBackoff > 0 {
			rc.MinBackoff = durationpb.New(task.RetryOptions.MinBackoff)
			hasRC = true
		}
		if task.RetryOptions.MaxBackoff > 0 {
			rc.MaxBackoff = durationpb.New(task.RetryOptions.MaxBackoff)
			hasRC = true
		}
		if task.RetryOptions.MaxDoublings > 0 || (task.RetryOptions.MaxDoublings == 0 && task.RetryOptions.ApplyZeroMaxDoublings) {
			rc.MaxDoublings = task.RetryOptions.MaxDoublings
			hasRC = true
		}
		if hasRC {
			taskObj.RetryConfig = rc
		}
	}

	return taskObj, taskName, nil
}

func addInCloudTasks(ctx context.Context, task *Task, queueName string) (*Task, error) {
	if queueName == "" {
		queueName = "default"
	}

	taskObj, taskName, err := buildCloudTaskProto(ctx, queueName, task)
	if err != nil {
		return nil, err
	}

	// In App Engine Datastore, external HTTP/gRPC Cloud Tasks RPCs cannot participate
	// in Datastore 2PC transactions. If we are running inside an active Datastore transaction,
	// we stage the task as a _AE_PendingCloudTask entity in Datastore under the transaction.
	// When the transaction commits, PostCommitHook dispatches the staged task to Cloud Tasks.
	if t := internal.TransactionFromContext(ctx); t != nil {
		handle := t.GetHandle()
		pendingTasksMu.Lock()
		if len(pendingTasks[handle]) >= maxTransactionalTasks {
			pendingTasksMu.Unlock()
			return nil, ErrTooManyTasksInTransaction
		}
		pendingTasksMu.Unlock()

		protoBytes, err := proto.Marshal(taskObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal proto for transactional task: %v", err)
		}
		key := datastore.NewIncompleteKey(ctx, "_AE_PendingCloudTask", nil)
		pendingTask := &PendingCloudTask{
			QueueName:        queueName,
			CloudTaskName:    taskName,
			CloudTaskPayload: string(protoBytes),
			Created:          time.Now(),
			Status:           "PENDING",
			RetryCount:       0,
			LastError:        "",
			HandledBySweeper: false,
			SdkLang:          "GO",
		}
		key, err = datastore.Put(ctx, key, pendingTask)
		if err != nil {
			return nil, fmt.Errorf("failed to save transactional task to Datastore: %v", err)
		}

		pendingTasksMu.Lock()
		pendingTasks[handle] = append(pendingTasks[handle], key.Encode())
		pendingTasksMu.Unlock()

		resultTask := *task
		resultTask.Name = taskName
		resultTask.Method = task.method()
		return &resultTask, nil
	}

	assignedName, err := sendTask(ctx, queueName, taskName, taskObj)
	if err != nil {
		return nil, err
	}

	resultTask := *task
	resultTask.Name = assignedName
	resultTask.Method = task.method()
	return &resultTask, nil
}

func addMultiInCloudTasks(ctx context.Context, tasks []*Task, queueName string) ([]*Task, error) {
	// If AddMulti is called inside a Datastore transaction, each task in the batch
	// is transactionally staged in Datastore via addInCloudTasks so that all tasks
	// commit atomically with the Datastore transaction.
	if t := internal.TransactionFromContext(ctx); t != nil {
		handle := t.GetHandle()
		pendingTasksMu.Lock()
		if len(pendingTasks[handle])+len(tasks) > maxTransactionalTasks {
			pendingTasksMu.Unlock()
			return nil, ErrTooManyTasksInTransaction
		}
		pendingTasksMu.Unlock()

		me, any := make(appengine.MultiError, len(tasks)), false
		results := make([]*Task, len(tasks))
		for i, task := range tasks {
			res, err := addInCloudTasks(ctx, task, queueName)
			if err != nil {
				me[i] = err
				any = true
			} else {
				results[i] = res
			}
		}
		if any {
			return results, me
		}
		return results, nil
	}

	fullQueueName, err := getQueuePath(ctx, queueName)
	if err != nil {
		return nil, err
	}

	me, any := make(appengine.MultiError, len(tasks)), false
	results := make([]*Task, len(tasks))

	client, err := getCloudTasksClient()
	if err != nil {
		return nil, err
	}

	chunkSize := batchCreateChunkSize
	for chunkStart := 0; chunkStart < len(tasks); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(tasks) {
			chunkEnd = len(tasks)
		}
		chunkTasks := tasks[chunkStart:chunkEnd]

		createReqs := make([]*taskspb.CreateTaskRequest, 0, len(chunkTasks))
		// reqTaskIdx[j] is the index in tasks of createReqs[j]. Tasks that fail
		// to build are not sent, so request indices can differ from task indices.
		reqTaskIdx := make([]int, 0, len(chunkTasks))
		for i, t := range chunkTasks {
			taskObj, taskName, err := buildCloudTaskProto(ctx, queueName, t)
			if err != nil {
				me[chunkStart+i] = err
				any = true
				continue
			}
			results[chunkStart+i] = new(Task)
			*results[chunkStart+i] = *t
			results[chunkStart+i].Name = taskName
			results[chunkStart+i].Method = t.method()

			createReqs = append(createReqs, &taskspb.CreateTaskRequest{
				Parent: fullQueueName,
				Task:   taskObj,
			})
			reqTaskIdx = append(reqTaskIdx, chunkStart+i)
		}
		if len(createReqs) == 0 {
			continue
		}

		batchReq := &taskspb.BatchCreateTasksRequest{
			Parent:   fullQueueName,
			Requests: createReqs,
		}

		op, err := client.BatchCreateTasks(ctx, batchReq)
		if err != nil {
			if isUnimplementedError(err) {
				for _, ti := range reqTaskIdx {
					res, err := addInCloudTasks(ctx, tasks[ti], queueName)
					if err != nil {
						me[ti] = err
						any = true
					} else {
						results[ti] = res
					}
				}
			} else {
				for _, ti := range reqTaskIdx {
					me[ti] = err
					any = true
				}
			}
			continue
		}
		if op == nil {
			continue
		}
		resp, waitErr := op.Wait(ctx)
		var failedReqs map[int32]*rpcstatus.Status
		if meta, metaErr := op.Metadata(); metaErr == nil && meta != nil {
			failedReqs = meta.FailedRequests
		}
		respIdx := 0
		for j, ti := range reqTaskIdx {
			if st, failed := failedReqs[int32(j)]; failed && st != nil && st.Code != 0 {
				me[ti] = mapOperationErrorCode(int(st.Code), st.Message, false)
				any = true
				continue
			}
			// The operation failed and this request has no per-request status
			// (e.g. DEADLINE_EXCEEDED, UNAUTHENTICATED): it was not created.
			if waitErr != nil {
				me[ti] = waitErr
				any = true
				continue
			}
			// resp.Tasks contains only the successfully created tasks, in request order.
			if resp != nil && respIdx < len(resp.Tasks) {
				createdTask := resp.Tasks[respIdx]
				respIdx++
				if createdTask != nil && createdTask.Name != "" && results[ti] != nil {
					if idx := strings.LastIndex(createdTask.Name, "/"); idx != -1 {
						results[ti].Name = createdTask.Name[idx+1:]
					} else {
						results[ti].Name = createdTask.Name
					}
				}
			}
		}
	}

	if any {
		return results, me
	}
	return results, nil
}

func deleteMultiInCloudTasks(ctx context.Context, tasks []*Task, queueName string) error {
	fullQueueName, err := getQueuePath(ctx, queueName)
	if err != nil {
		return err
	}

	client, err := getCloudTasksClient()
	if err != nil {
		return err
	}

	me, any := make(appengine.MultiError, len(tasks)), false

	chunkSize := batchDeleteChunkSize
	for chunkStart := 0; chunkStart < len(tasks); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(tasks) {
			chunkEnd = len(tasks)
		}
		chunkTasks := tasks[chunkStart:chunkEnd]

		names := make([]string, len(chunkTasks))
		for i, t := range chunkTasks {
			names[i] = fmt.Sprintf("%s/tasks/%s", fullQueueName, t.Name)
		}

		batchReq := &taskspb.BatchDeleteTasksRequest{
			Parent: fullQueueName,
			Names:  names,
		}

		op, err := client.BatchDeleteTasks(ctx, batchReq)
		if err != nil {
			for i := range chunkTasks {
				me[chunkStart+i] = err
				any = true
			}
		} else if op != nil {
			waitErr := op.Wait(ctx)
			var failedReqs map[int32]*rpcstatus.Status
			if meta, metaErr := op.Metadata(); metaErr == nil && meta != nil {
				failedReqs = meta.FailedRequests
			}
			for i := range chunkTasks {
				if st, failed := failedReqs[int32(i)]; failed && st != nil && st.Code != 0 {
					me[chunkStart+i] = mapOperationErrorCode(int(st.Code), st.Message, true)
					any = true
					continue
				}
				// The operation failed and this request has no per-request status:
				// do not report the delete as successful.
				if waitErr != nil {
					me[chunkStart+i] = waitErr
					any = true
				}
			}
		}
	}

	if any {
		return me
	}
	return nil
}

func mapOperationErrorCode(code int, msg string, isDelete bool) error {
	lowerMsg := strings.ToLower(msg)
	isNotFound := code == grpcNotFound || code == httpNotFound || strings.Contains(lowerMsg, "not found") || strings.Contains(lowerMsg, "unknown")
	isAlreadyExists := code == grpcAlreadyExists || code == httpAlreadyExists || strings.Contains(lowerMsg, "already exists")

	if isDelete && isNotFound {
		return newUnknownTaskError(msg)
	}
	if isAlreadyExists {
		return ErrTaskAlreadyAdded
	}
	if isNotFound {
		// On create, NOT_FOUND refers to the parent queue, not the task.
		return &internal.APIError{
			Service: "taskqueue",
			Detail:  msg,
			Code:    int32(pb.TaskQueueServiceError_UNKNOWN_QUEUE),
		}
	}
	return fmt.Errorf("cloud tasks operation failed (%d): %s", code, msg)
}

func queueStatsInCloudTasks(ctx context.Context, queueNames []string) ([]QueueStatistics, error) {
	// QueueStats is retained on v2beta3 as it is out of scope for v2 GA
	client, err := getCloudTasksBetaClient()
	if err != nil {
		return nil, err
	}

	qs := make([]QueueStatistics, len(queueNames))
	for i, q := range queueNames {
		fullQueueName, err := getQueuePath(ctx, q)
		if err != nil {
			return nil, err
		}
		req := &taskspbbeta.GetQueueRequest{
			Name: fullQueueName,
			ReadMask: &fieldmaskpb.FieldMask{
				Paths: []string{"stats"},
			},
		}
		queue, err := client.GetQueue(ctx, req)
		if err != nil {
			return nil, err
		}
		if queue != nil && queue.Stats != nil {
			qs[i] = QueueStatistics{
				Tasks:           int(queue.Stats.TasksCount),
				Executed1Minute: int(queue.Stats.ExecutedLastMinuteCount),
				InFlight:        int(queue.Stats.ConcurrentDispatchesCount),
				EnforcedRate:    queue.Stats.EffectiveExecutionRate,
			}
			if queue.Stats.OldestEstimatedArrivalTime != nil {
				qs[i].OldestETA = queue.Stats.OldestEstimatedArrivalTime.AsTime()
			}
		}
	}
	return qs, nil
}
