// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
Package taskqueue provides a client for App Engine's taskqueue service.
Using this service, applications may perform work outside a user's request.

A Task may be constructed manually; alternatively, since the most common
taskqueue operation is to add a single POST task, NewPOSTTask makes it easy.

	t := taskqueue.NewPOSTTask("/worker", url.Values{
		"key": {key},
	})
	taskqueue.Add(c, t, "") // add t to the default queue
*/
package taskqueue // import "google.golang.org/appengine/taskqueue"

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	cloudtasksv2beta2 "google.golang.org/api/cloudtasks/v2beta2"
	"google.golang.org/api/googleapi"
	"github.com/golang/protobuf/proto"

	"google.golang.org/appengine"
	"google.golang.org/appengine/internal"
	dspb "google.golang.org/appengine/internal/datastore"
	pb "google.golang.org/appengine/internal/taskqueue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	gaePushqueueBackendEnv = "GAE_PUSHQUEUE_BACKEND"
	cloudTaskBackend       = "CLOUD_TASK"
	legacyBackend          = "LEGACY"

	googleCloudProjectEnv = "GOOGLE_CLOUD_PROJECT"
	cloudTasksLocationEnv = "CLOUD_TASKS_LOCATION"

	queuePathTemplate      = "projects/%s/locations/%s/queues/%s"
	queuePurgePathTemplate = "projects/%s/locations/%s/queues/%s:purge"
	taskPathTemplate       = "projects/%s/locations/%s/queues/%s/tasks/%s"
)

var (
	ctClient     *cloudtasks.Client
	ctClientOnce sync.Once

	ctRESTClient     *cloudtasksv2beta2.Service
	ctRESTClientOnce sync.Once

	// BackendUsed indicates the backend that was used for the last operation.
	// Possible values are "CLOUD_TASKS" or "LEGACY".
	BackendUsed     string
	ctClientErr     error
	ctRESTClientErr error
)

const (
	cloudTasksBackendName = "CLOUD_TASKS"
	legacyBackendName     = "LEGACY"
)

func shouldUseCloudTasks() bool {
	backend := os.Getenv(gaePushqueueBackendEnv)
	if backend == "" {
		backend = legacyBackend
	}

	if backend == cloudTaskBackend {
		return true
	}

	if backend != legacyBackend {
		log.Printf("Unrecognized value for %s: %s. Defaulting to LEGACY.", gaePushqueueBackendEnv, backend)
	}

	return false
}

func getCloudTasksProjectAndLocation() (string, string) {
	project := os.Getenv(googleCloudProjectEnv)
	location := os.Getenv(cloudTasksLocationEnv)
	if location == "" {
		location = "us-central1"
	}
	return project, location
}

func getCloudTasksQueuePath(queueName string) string {
	project, location := getCloudTasksProjectAndLocation()
	return fmt.Sprintf(queuePathTemplate, project, location, queueName)
}

func getCloudTasksQueuePurgePath(queueName string) string {
	project, location := getCloudTasksProjectAndLocation()
	return fmt.Sprintf(queuePurgePathTemplate, project, location, queueName)
}

func getCloudTasksTaskPath(queueName, taskName string) string {
	project, location := getCloudTasksProjectAndLocation()
	return fmt.Sprintf(taskPathTemplate, project, location, queueName, taskName)
}

func getCTClient(ctx context.Context) (*cloudtasks.Client, error) {
	ctClientOnce.Do(func() {
		var err error
		ctClient, err = cloudtasks.NewClient(context.Background())
		if err != nil {
			ctClientErr = err
		}
	})
	return ctClient, ctClientErr
}

func getCTRESTClient(ctx context.Context) (*cloudtasksv2beta2.Service, error) {
	ctRESTClientOnce.Do(func() {
		var err error
		ctRESTClient, err = cloudtasksv2beta2.NewService(context.Background())
		if err != nil {
			ctRESTClientErr = err
		}
	})
	return ctRESTClient, ctRESTClientErr
}

var (
	// ErrTaskAlreadyAdded is the error returned by Add and AddMulti when a task has already been added with a particular name.
	ErrTaskAlreadyAdded = errors.New("taskqueue: task has already been added")
)

// RetryOptions let you control whether to retry a task and the backoff intervals between tries.
type RetryOptions struct {
	// Number of tries/leases after which the task fails permanently and is deleted.
	// If AgeLimit is also set, both limits must be exceeded for the task to fail permanently.
	RetryLimit int32

	// Maximum time allowed since the task's first try before the task fails permanently and is deleted (only for push tasks).
	// If RetryLimit is also set, both limits must be exceeded for the task to fail permanently.
	AgeLimit time.Duration

	// Minimum time between successive tries (only for push tasks).
	MinBackoff time.Duration

	// Maximum time between successive tries (only for push tasks).
	MaxBackoff time.Duration

	// Maximum number of times to double the interval between successive tries before the intervals increase linearly (only for push tasks).
	MaxDoublings int32

	// If MaxDoublings is zero, set ApplyZeroMaxDoublings to true to override the default non-zero value.
	// Otherwise a zero MaxDoublings is ignored and the default is used.
	ApplyZeroMaxDoublings bool
}

// toRetryParameters converts RetryOptions to pb.TaskQueueRetryParameters.
func (opt *RetryOptions) toRetryParameters() *pb.TaskQueueRetryParameters {
	params := &pb.TaskQueueRetryParameters{}
	if opt.RetryLimit > 0 {
		params.RetryLimit = proto.Int32(opt.RetryLimit)
	}
	if opt.AgeLimit > 0 {
		params.AgeLimitSec = proto.Int64(int64(opt.AgeLimit.Seconds()))
	}
	if opt.MinBackoff > 0 {
		params.MinBackoffSec = proto.Float64(opt.MinBackoff.Seconds())
	}
	if opt.MaxBackoff > 0 {
		params.MaxBackoffSec = proto.Float64(opt.MaxBackoff.Seconds())
	}
	if opt.MaxDoublings > 0 || (opt.MaxDoublings == 0 && opt.ApplyZeroMaxDoublings) {
		params.MaxDoublings = proto.Int32(opt.MaxDoublings)
	}
	return params
}

// A Task represents a task to be executed.
type Task struct {
	// Path is the worker URL for the task.
	// If unset, it will default to /_ah/queue/<queue_name>.
	Path string

	// Payload is the data for the task.
	// This will be delivered as the HTTP request body.
	// It is only used when Method is POST, PUT or PULL.
	// url.Values' Encode method may be used to generate this for POST requests.
	Payload []byte

	// Additional HTTP headers to pass at the task's execution time.
	// To schedule the task to be run with an alternate app version
	// or backend, set the "Host" header.
	Header http.Header

	// Method is the HTTP method for the task ("GET", "POST", etc.),
	// or "PULL" if this is task is destined for a pull-based queue.
	// If empty, this defaults to "POST".
	Method string

	// A name for the task.
	// If empty, a name will be chosen.
	Name string

	// Delay specifies the duration the task queue service must wait
	// before executing the task.
	// Either Delay or ETA may be set, but not both.
	Delay time.Duration

	// ETA specifies the earliest time a task may be executed (push queues)
	// or leased (pull queues).
	// Either Delay or ETA may be set, but not both.
	ETA time.Time

	// The number of times the task has been dispatched or leased.
	RetryCount int32

	// Tag for the task. Only used when Method is PULL.
	Tag string

	// Retry options for this task. May be nil.
	RetryOptions *RetryOptions

	// BackendUsed is the backend used to process the task.
	// It is either "CLOUD_TASKS" or "LEGACY".
	BackendUsed string
}

func (t *Task) method() string {
	if t.Method == "" {
		return "POST"
	}
	return t.Method
}

// NewPOSTTask creates a Task that will POST to a path with the given form data.
func NewPOSTTask(path string, params url.Values) *Task {
	h := make(http.Header)
	h.Set("Content-Type", "application/x-www-form-urlencoded")
	return &Task{
		Path:    path,
		Payload: []byte(params.Encode()),
		Header:  h,
		Method:  "POST",
	}
}

// RequestHeaders are the special HTTP request headers available to push task
// HTTP request handlers. These headers are set internally by App Engine.
// See https://cloud.google.com/appengine/docs/standard/go/taskqueue/push/creating-handlers#reading_request_headers
// for a description of the fields.
type RequestHeaders struct {
	QueueName          string
	TaskName           string
	TaskRetryCount     int64
	TaskExecutionCount int64
	TaskETA            time.Time

	TaskPreviousResponse int
	TaskRetryReason      string
	FailFast             bool
}

// ParseRequestHeaders parses the special HTTP request headers available to push
// task request handlers. This function silently ignores values of the wrong
// format.
func ParseRequestHeaders(h http.Header) *RequestHeaders {
	ret := &RequestHeaders{
		QueueName: h.Get("X-AppEngine-QueueName"),
		TaskName:  h.Get("X-AppEngine-TaskName"),
	}

	ret.TaskRetryCount, _ = strconv.ParseInt(h.Get("X-AppEngine-TaskRetryCount"), 10, 64)
	ret.TaskExecutionCount, _ = strconv.ParseInt(h.Get("X-AppEngine-TaskExecutionCount"), 10, 64)

	etaSecs, _ := strconv.ParseInt(h.Get("X-AppEngine-TaskETA"), 10, 64)
	if etaSecs != 0 {
		ret.TaskETA = time.Unix(etaSecs, 0)
	}

	ret.TaskPreviousResponse, _ = strconv.Atoi(h.Get("X-AppEngine-TaskPreviousResponse"))
	ret.TaskRetryReason = h.Get("X-AppEngine-TaskRetryReason")
	if h.Get("X-AppEngine-FailFast") != "" {
		ret.FailFast = true
	}

	return ret
}

var (
	currentNamespace = http.CanonicalHeaderKey("X-AppEngine-Current-Namespace")
	defaultNamespace = http.CanonicalHeaderKey("X-AppEngine-Default-Namespace")
)

func getDefaultNamespace(ctx context.Context) string {
	return internal.IncomingHeaders(ctx).Get(defaultNamespace)
}

func newAddReq(c context.Context, task *Task, queueName string) (*pb.TaskQueueAddRequest, error) {
	if queueName == "" {
		queueName = "default"
	}
	path := task.Path
	if path == "" {
		path = "/_ah/queue/" + queueName
	}
	eta := task.ETA
	if eta.IsZero() {
		eta = time.Now().Add(task.Delay)
	} else if task.Delay != 0 {
		panic("taskqueue: both Delay and ETA are set")
	}
	req := &pb.TaskQueueAddRequest{
		QueueName: []byte(queueName),
		TaskName:  []byte(task.Name),
		EtaUsec:   proto.Int64(eta.UnixNano() / 1e3),
	}
	method := task.method()
	if method == "PULL" {
		// Pull-based task
		req.Body = task.Payload
		req.Mode = pb.TaskQueueMode_PULL.Enum()
		if task.Tag != "" {
			req.Tag = []byte(task.Tag)
		}
	} else {
		// HTTP-based task
		if v, ok := pb.TaskQueueAddRequest_RequestMethod_value[method]; ok {
			req.Method = pb.TaskQueueAddRequest_RequestMethod(v).Enum()
		} else {
			return nil, fmt.Errorf("taskqueue: bad method %q", method)
		}
		req.Url = []byte(path)
		for k, vs := range task.Header {
			for _, v := range vs {
				req.Header = append(req.Header, &pb.TaskQueueAddRequest_Header{
					Key:   []byte(k),
					Value: []byte(v),
				})
			}
		}
		if method == "POST" || method == "PUT" {
			req.Body = task.Payload
		}

		// Namespace headers.
		if _, ok := task.Header[currentNamespace]; !ok {
			// Fetch the current namespace of this request.
			ns := internal.NamespaceFromContext(c)
			req.Header = append(req.Header, &pb.TaskQueueAddRequest_Header{
				Key:   []byte(currentNamespace),
				Value: []byte(ns),
			})
		}
		if _, ok := task.Header[defaultNamespace]; !ok {
			// Fetch the X-AppEngine-Default-Namespace header of this request.
			if ns := getDefaultNamespace(c); ns != "" {
				req.Header = append(req.Header, &pb.TaskQueueAddRequest_Header{
					Key:   []byte(defaultNamespace),
					Value: []byte(ns),
				})
			}
		}
	}

	if task.RetryOptions != nil {
		req.RetryParameters = task.RetryOptions.toRetryParameters()
	}

	return req, nil
}

var alreadyAddedErrors = map[pb.TaskQueueServiceError_ErrorCode]bool{
	pb.TaskQueueServiceError_TASK_ALREADY_EXISTS: true,
	pb.TaskQueueServiceError_TOMBSTONED_TASK:     true,
}

func addCloudTasks(c context.Context, task *Task, queueName string) (*Task, error) {
	if queueName == "" {
		queueName = "default"
	}
	client, err := getCTClient(c)
	if err != nil {
		return nil, err
	}

	queuePath := getCloudTasksQueuePath(queueName)

	ctTask := &taskspb.Task{}

	if task.Name != "" {
		ctTask.Name = getCloudTasksTaskPath(queueName, task.Name)
	}

	if !task.ETA.IsZero() {
		ctTask.ScheduleTime = timestamppb.New(task.ETA)
	} else if task.Delay != 0 {
		ctTask.ScheduleTime = timestamppb.New(time.Now().Add(task.Delay))
	}

	method := task.method()
	if method == "PULL" {
		return nil, errors.New("taskqueue: pull tasks are not supported in Cloud Tasks")
	}

	httpMethodMap := map[string]taskspb.HttpMethod{
		"GET":    taskspb.HttpMethod_GET,
		"POST":   taskspb.HttpMethod_POST,
		"HEAD":   taskspb.HttpMethod_HEAD,
		"PUT":    taskspb.HttpMethod_PUT,
		"DELETE": taskspb.HttpMethod_DELETE,
	}

	ctHttpMethod, ok := httpMethodMap[method]
	if !ok {
		ctHttpMethod = taskspb.HttpMethod_POST
	}

	relativeURI := task.Path
	if relativeURI == "" {
		relativeURI = "/_ah/queue/" + queueName
	}

	headers := make(map[string]string)
	for k, vs := range task.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	appEngineReq := &taskspb.AppEngineHttpRequest{
		HttpMethod:  ctHttpMethod,
		RelativeUri: relativeURI,
		Headers:     headers,
	}

	if task.Payload != nil {
		appEngineReq.Body = task.Payload
	}

	// Namespace headers.
	if _, ok := task.Header[currentNamespace]; !ok {
		// Fetch the current namespace of this request.
		if ns := internal.NamespaceFromContext(c); ns != "" {
			if appEngineReq.Headers == nil {
				appEngineReq.Headers = make(map[string]string)
			}
			appEngineReq.Headers[currentNamespace] = ns
		}
	}
	if _, ok := task.Header[defaultNamespace]; !ok {
		// Fetch the X-AppEngine-Default-Namespace header of this request.
		if ns := getDefaultNamespace(c); ns != "" {
			if appEngineReq.Headers == nil {
				appEngineReq.Headers = make(map[string]string)
			}
			appEngineReq.Headers[defaultNamespace] = ns
		}
	}

	ctTask.MessageType = &taskspb.Task_AppEngineHttpRequest{
		AppEngineHttpRequest: appEngineReq,
	}

	req := &taskspb.CreateTaskRequest{
		Parent: queuePath,
		Task:   ctTask,
	}

	respTask, err := client.CreateTask(c, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.AlreadyExists:
				return nil, ErrTaskAlreadyAdded
			case codes.NotFound:
				return nil, errors.New("taskqueue: UNKNOWN_QUEUE")
			case codes.PermissionDenied:
				return nil, errors.New("taskqueue: PERMISSION_DENIED")
			case codes.DeadlineExceeded, codes.Unavailable:
				return nil, errors.New("taskqueue: TRANSIENT_ERROR")
			}
		}
		return nil, err
	}

	resultTask := *task
	resultTask.Method = method
	resultTask.BackendUsed = cloudTasksBackendName
	if task.Name == "" && respTask.Name != "" {
		// Name from Cloud Tasks API is the full path. We just want the last part.
		resultTask.Name = path.Base(respTask.Name)
	}
	return &resultTask, nil
}

// Add adds the task to a named queue.
// An empty queue name means that the default queue will be used.
// Add returns an equivalent Task with defaults filled in, including setting
// the task's Name field to the chosen name if the original was empty.
func Add(c context.Context, task *Task, queueName string) (*Task, error) {
	if shouldUseCloudTasks() {
		BackendUsed = cloudTasksBackendName
		return addCloudTasks(c, task, queueName)
	}
	BackendUsed = legacyBackendName

	req, err := newAddReq(c, task, queueName)
	if err != nil {
		return nil, err
	}
	res := &pb.TaskQueueAddResponse{}
	if err := internal.Call(c, "taskqueue", "Add", req, res); err != nil {
		apiErr, ok := err.(*internal.APIError)
		if ok && alreadyAddedErrors[pb.TaskQueueServiceError_ErrorCode(apiErr.Code)] {
			return nil, ErrTaskAlreadyAdded
		}
		return nil, err
	}
	resultTask := *task
	resultTask.Method = task.method()
	resultTask.BackendUsed = BackendUsed
	if task.Name == "" {
		resultTask.Name = string(res.ChosenTaskName)
	}
	return &resultTask, nil
}

// AddMulti adds multiple tasks to a named queue.
// An empty queue name means that the default queue will be used.
// AddMulti returns a slice of equivalent tasks with defaults filled in, including setting
// each task's Name field to the chosen name if the original was empty.
// If a given task is badly formed or could not be added, an appengine.MultiError is returned.
func AddMulti(c context.Context, tasks []*Task, queueName string) ([]*Task, error) {
	if shouldUseCloudTasks() {
		BackendUsed = cloudTasksBackendName
		if len(tasks) > 1 {
			return nil, errors.New("taskqueue: Batch operations are not supported in Cloud Tasks path yet")
		}
		if len(tasks) == 0 {
			return []*Task{}, nil
		}
		t, err := addCloudTasks(c, tasks[0], queueName)
		if err != nil {
			me := make(appengine.MultiError, 1)
			me[0] = err
			return nil, me
		}
		return []*Task{t}, nil
	}
	BackendUsed = legacyBackendName

	req := &pb.TaskQueueBulkAddRequest{
		AddRequest: make([]*pb.TaskQueueAddRequest, len(tasks)),
	}
	me, any := make(appengine.MultiError, len(tasks)), false
	for i, t := range tasks {
		req.AddRequest[i], me[i] = newAddReq(c, t, queueName)
		any = any || me[i] != nil
	}
	if any {
		return nil, me
	}
	res := &pb.TaskQueueBulkAddResponse{}
	if err := internal.Call(c, "taskqueue", "BulkAdd", req, res); err != nil {
		return nil, err
	}
	if len(res.Taskresult) != len(tasks) {
		return nil, errors.New("taskqueue: server error")
	}
	tasksOut := make([]*Task, len(tasks))
	for i, tr := range res.Taskresult {
		tasksOut[i] = new(Task)
		*tasksOut[i] = *tasks[i]
		tasksOut[i].Method = tasksOut[i].method()
		tasksOut[i].BackendUsed = BackendUsed
		if tasksOut[i].Name == "" {
			tasksOut[i].Name = string(tr.ChosenTaskName)
		}
		if *tr.Result != pb.TaskQueueServiceError_OK {
			if alreadyAddedErrors[*tr.Result] {
				me[i] = ErrTaskAlreadyAdded
			} else {
				me[i] = &internal.APIError{
					Service: "taskqueue",
					Code:    int32(*tr.Result),
				}
			}
			any = true
		}
	}
	if any {
		return tasksOut, me
	}
	return tasksOut, nil
}

// Delete deletes a task from a named queue.
func Delete(c context.Context, task *Task, queueName string) error {
	err := DeleteMulti(c, []*Task{task}, queueName)
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

// DeleteMulti deletes multiple tasks from a named queue.
// If a given task could not be deleted, an appengine.MultiError is returned.
// Each task is deleted independently; one may fail to delete while the others
// are successfully deleted.
func DeleteMulti(c context.Context, tasks []*Task, queueName string) error {
	if queueName == "" {
		queueName = "default"
	}

	if shouldUseCloudTasks() {
		BackendUsed = cloudTasksBackendName
		if len(tasks) > 1 {
			return errors.New("taskqueue: Batch delete is not supported in Cloud Tasks path yet")
		}
		if len(tasks) == 0 {
			return nil
		}
		client, err := getCTClient(c)
		if err != nil {
			return err
		}
		task := tasks[0]
		if task.Name == "" {
			return errors.New("taskqueue: A task name must be specified for a task")
		}

		path := getCloudTasksTaskPath(queueName, task.Name)
		req := &taskspb.DeleteTaskRequest{
			Name: path,
		}

		err = client.DeleteTask(c, req)
		if err != nil {
			me := make(appengine.MultiError, 1)
			me[0] = err
			return me
		}
		return nil
	}
	BackendUsed = legacyBackendName

	taskNames := make([][]byte, len(tasks))
	for i, t := range tasks {
		taskNames[i] = []byte(t.Name)
	}

	req := &pb.TaskQueueDeleteRequest{
		QueueName: []byte(queueName),
		TaskName:  taskNames,
	}
	res := &pb.TaskQueueDeleteResponse{}
	if err := internal.Call(c, "taskqueue", "Delete", req, res); err != nil {
		return err
	}
	if a, b := len(req.TaskName), len(res.Result); a != b {
		return fmt.Errorf("taskqueue: internal error: requested deletion of %d tasks, got %d results", a, b)
	}
	me, any := make(appengine.MultiError, len(res.Result)), false
	for i, ec := range res.Result {
		if ec != pb.TaskQueueServiceError_OK {
			me[i] = &internal.APIError{
				Service: "taskqueue",
				Code:    int32(ec),
			}
			any = true
		}
	}
	if any {
		return me
	}
	return nil
}

func lease(c context.Context, maxTasks int, queueName string, leaseTime int, groupByTag bool, tag []byte) ([]*Task, error) {
	if queueName == "" {
		queueName = "default"
	}
	req := &pb.TaskQueueQueryAndOwnTasksRequest{
		QueueName:    []byte(queueName),
		LeaseSeconds: proto.Float64(float64(leaseTime)),
		MaxTasks:     proto.Int64(int64(maxTasks)),
		GroupByTag:   proto.Bool(groupByTag),
		Tag:          tag,
	}
	res := &pb.TaskQueueQueryAndOwnTasksResponse{}
	if err := internal.Call(c, "taskqueue", "QueryAndOwnTasks", req, res); err != nil {
		return nil, err
	}
	tasks := make([]*Task, len(res.Task))
	for i, t := range res.Task {
		tasks[i] = &Task{
			Payload:    t.Body,
			Name:       string(t.TaskName),
			Method:     "PULL",
			ETA:        time.Unix(0, *t.EtaUsec*1e3),
			RetryCount: *t.RetryCount,
			Tag:        string(t.Tag),
		}
	}
	return tasks, nil
}

// Lease leases tasks from a queue.
// leaseTime is in seconds.
// The number of tasks fetched will be at most maxTasks.
func Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]*Task, error) {
	return lease(c, maxTasks, queueName, leaseTime, false, nil)
}

// LeaseByTag leases tasks from a queue, grouped by tag.
// If tag is empty, then the returned tasks are grouped by the tag of the task with earliest ETA.
// leaseTime is in seconds.
// The number of tasks fetched will be at most maxTasks.
func LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]*Task, error) {
	return lease(c, maxTasks, queueName, leaseTime, true, []byte(tag))
}

// Purge removes all tasks from a queue.
func Purge(c context.Context, queueName string) error {
	if queueName == "" {
		queueName = "default"
	}

	if shouldUseCloudTasks() {
		BackendUsed = cloudTasksBackendName
		client, err := getCTClient(c)
		if err != nil {
			return err
		}

		path := getCloudTasksQueuePurgePath(queueName)
		req := &taskspb.PurgeQueueRequest{
			Name: path,
		}

		_, err = client.PurgeQueue(c, req)
		return err
	}
	BackendUsed = legacyBackendName

	req := &pb.TaskQueuePurgeQueueRequest{
		QueueName: []byte(queueName),
	}
	res := &pb.TaskQueuePurgeQueueResponse{}
	return internal.Call(c, "taskqueue", "PurgeQueue", req, res)
}

// ModifyLease modifies the lease of a task.
// Used to request more processing time, or to abandon processing.
// leaseTime is in seconds and must not be negative.
func ModifyLease(c context.Context, task *Task, queueName string, leaseTime int) error {
	if queueName == "" {
		queueName = "default"
	}
	req := &pb.TaskQueueModifyTaskLeaseRequest{
		QueueName:    []byte(queueName),
		TaskName:     []byte(task.Name),
		EtaUsec:      proto.Int64(task.ETA.UnixNano() / 1e3), // Used to verify ownership.
		LeaseSeconds: proto.Float64(float64(leaseTime)),
	}
	res := &pb.TaskQueueModifyTaskLeaseResponse{}
	if err := internal.Call(c, "taskqueue", "ModifyTaskLease", req, res); err != nil {
		return err
	}
	task.ETA = time.Unix(0, *res.UpdatedEtaUsec*1e3)
	return nil
}

// QueueStatistics represents statistics about a single task queue.
type QueueStatistics struct {
	Tasks     int       // may be an approximation
	OldestETA time.Time // zero if there are no pending tasks

	Executed1Minute int     // tasks executed in the last minute
	InFlight        int     // tasks executing now
	EnforcedRate    float64 // requests per second

	// BackendUsed is the backend used to fetch the statistics.
	// It is either "CLOUD_TASKS" or "LEGACY".
	BackendUsed string
}

func queueStatsCloudTasks(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	service, err := getCTRESTClient(c)
	if err != nil {
		return nil, err
	}

	qs := make([]QueueStatistics, len(queueNames))
	for i, q := range queueNames {
		if q == "" {
			q = "default"
		}
		qPath := getCloudTasksQueuePath(q)
		ctQueue, err := service.Projects.Locations.Queues.Get(qPath).ReadMask("stats").Context(c).Do()
		if err != nil {
			if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == http.StatusNotFound {
				// Equivalent to UnknownQueueError in python
				qs[i] = QueueStatistics{Tasks: 0, OldestETA: time.Time{}, BackendUsed: cloudTasksBackendName}
				continue
			}
			return nil, err
		} else if ctQueue.Stats != nil {
			qs[i] = QueueStatistics{
				Tasks:           int(ctQueue.Stats.TasksCount),
				Executed1Minute: int(ctQueue.Stats.ExecutedLastMinuteCount),
				InFlight:        int(ctQueue.Stats.ConcurrentDispatchesCount),
				EnforcedRate:    ctQueue.Stats.EffectiveExecutionRate,
				BackendUsed:     cloudTasksBackendName,
			}
			if ctQueue.Stats.OldestEstimatedArrivalTime != "" {
				t, err := time.Parse(time.RFC3339, ctQueue.Stats.OldestEstimatedArrivalTime)
				if err == nil {
					qs[i].OldestETA = t
				}
			}
		} else {
			qs[i] = QueueStatistics{Tasks: 0, OldestETA: time.Time{}, BackendUsed: cloudTasksBackendName}
		}
	}
	return qs, nil
}

// QueueStats retrieves statistics about queues.
func QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	if shouldUseCloudTasks() {
		BackendUsed = cloudTasksBackendName
		return queueStatsCloudTasks(c, queueNames)
	}
	BackendUsed = legacyBackendName

	req := &pb.TaskQueueFetchQueueStatsRequest{
		QueueName: make([][]byte, len(queueNames)),
	}
	for i, q := range queueNames {
		if q == "" {
			q = "default"
		}
		req.QueueName[i] = []byte(q)
	}
	res := &pb.TaskQueueFetchQueueStatsResponse{}
	if err := internal.Call(c, "taskqueue", "FetchQueueStats", req, res); err != nil {
		return nil, err
	}
	qs := make([]QueueStatistics, len(res.Queuestats))
	for i, qsg := range res.Queuestats {
		qs[i] = QueueStatistics{
			Tasks:       int(*qsg.NumTasks),
			BackendUsed: BackendUsed,
		}
		if eta := *qsg.OldestEtaUsec; eta > -1 {
			qs[i].OldestETA = time.Unix(0, eta*1e3)
		}
		if si := qsg.ScannerInfo; si != nil {
			qs[i].Executed1Minute = int(*si.ExecutedLastMinute)
			qs[i].InFlight = int(si.GetRequestsInFlight())
			qs[i].EnforcedRate = si.GetEnforcedRate()
		}
	}
	return qs, nil
}

func setTransaction(x *pb.TaskQueueAddRequest, t *dspb.Transaction) {
	x.Transaction = t
}

func init() {
	internal.RegisterErrorCodeMap("taskqueue", pb.TaskQueueServiceError_ErrorCode_name)

	// Datastore error codes are shifted by DATASTORE_ERROR when presented through taskqueue.
	dsCode := int32(pb.TaskQueueServiceError_DATASTORE_ERROR) + int32(dspb.Error_TIMEOUT)
	internal.RegisterTimeoutErrorCode("taskqueue", dsCode)

	// Transaction registration.
	internal.RegisterTransactionSetter(setTransaction)
	internal.RegisterTransactionSetter(func(x *pb.TaskQueueBulkAddRequest, t *dspb.Transaction) {
		for _, req := range x.AddRequest {
			setTransaction(req, t)
		}
	})
}
