package yunikorn

import (
	"encoding/json"
	"fmt"
	"k8s.io/client-go/rest"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/kubeflow/spark-operator/pkg/batchscheduler/interface"
	corev1 "k8s.io/api/core/v1"
)

var _ schedulerinterface.BatchScheduler = (*YunikornScheduler)(nil)

const (
	SchedulerName = "yunikorn"

	DriverTaskGroupName   = "spark-driver"
	ExecutorTaskGroupName = "spark-executor"

	TaskGroupNameAnnotation = "yunikorn.apache.org/task-group-name"
	TaskGroupsAnnotation    = "yunikorn.apache.org/task-groups"

	QueueLabel = "queue"
)

// This struct has been defined separately rather than imported so that tags can be included for JSON marshalling
// https://github.com/apache/yunikorn-k8shim/blob/207e4031c6484c965fca4018b6b8176afc5956b4/pkg/cache/amprotocol.go#L47-L56
type taskGroup struct {
	Name         string                         `json:"name"`
	MinMember    int32                          `json:"minMember"`
	MinResource  map[corev1.ResourceName]string `json:"minResource,omitempty"`
	NodeSelector map[string]string              `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration            `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity               `json:"affinity,omitempty"`
	Labels       map[string]string              `json:"labels,omitempty"`
}

type YunikornScheduler struct{}

func GetPluginName() string {
	return SchedulerName
}

func (v *YunikornScheduler) Name() string {
	return GetPluginName()
}

func (v *YunikornScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	return true
}

func (v *YunikornScheduler) DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error {
	driverMinResources, err := driverPodResourceUsage(app)
	if err != nil {
		return fmt.Errorf("failed to calculate driver pod resource usage: %w", err)
	}

	taskGroups := []taskGroup{
		{
			Name:         DriverTaskGroupName,
			MinMember:    1,
			MinResource:  driverMinResources,
			NodeSelector: mergeMaps(app.Spec.NodeSelector, app.Spec.Driver.NodeSelector),
			Tolerations:  app.Spec.Driver.Tolerations,
			Affinity:     app.Spec.Driver.Affinity,
			Labels:       app.Spec.Driver.Labels,
		},
	}

	// A minMember of zero is not a valid config for a Yunikorn task group,
	// so we should leave out the executor task group completely
	// if the initial number of executors is zero
	if initialExecutors := getInitialExecutors(app); initialExecutors > 0 {
		executorMinResources, err := executorPodResourceUsage(app)
		if err != nil {
			return fmt.Errorf("failed to calculate executor pod resource usage: %w", err)
		}

		taskGroups = append(taskGroups, taskGroup{
			Name:         ExecutorTaskGroupName,
			MinMember:    initialExecutors,
			MinResource:  executorMinResources,
			NodeSelector: mergeMaps(app.Spec.NodeSelector, app.Spec.Executor.NodeSelector),
			Tolerations:  app.Spec.Executor.Tolerations,
			Affinity:     app.Spec.Executor.Affinity,
			Labels:       app.Spec.Executor.Labels,
		})
	}

	if err := addTaskGroupAnnotations(app, taskGroups); err != nil {
		return fmt.Errorf("failed to add task group annotations: %w", err)
	}
	addQueueLabels(app)

	return nil
}

func (v *YunikornScheduler) CleanupOnCompletion(app *v1beta2.SparkApplication) error {
	// Yunikorn does not involve any additional crds.. just skip it
	return nil
}

func addTaskGroupAnnotations(app *v1beta2.SparkApplication, taskGroups []taskGroup) error {
	marshalledTaskGroups, err := json.Marshal(taskGroups)
	if err != nil {
		return fmt.Errorf("failed to marshal taskGroups: %w", err)
	}

	if app.Spec.Driver.Annotations == nil {
		app.Spec.Driver.Annotations = make(map[string]string)
	}

	if app.Spec.Executor.Annotations == nil {
		app.Spec.Executor.Annotations = make(map[string]string)
	}

	app.Spec.Driver.Annotations[TaskGroupNameAnnotation] = DriverTaskGroupName
	app.Spec.Executor.Annotations[TaskGroupNameAnnotation] = ExecutorTaskGroupName

	// The task group annotation only needs to be present on the originating pod
	app.Spec.Driver.Annotations[TaskGroupsAnnotation] = string(marshalledTaskGroups)

	return nil
}

func addQueueLabels(app *v1beta2.SparkApplication) {
	if app.Spec.BatchSchedulerOptions != nil && app.Spec.BatchSchedulerOptions.Queue != nil {
		if app.Spec.Driver.Labels == nil {
			app.Spec.Driver.Labels = make(map[string]string)
		}

		if app.Spec.Executor.Labels == nil {
			app.Spec.Executor.Labels = make(map[string]string)
		}

		app.Spec.Driver.Labels[QueueLabel] = *app.Spec.BatchSchedulerOptions.Queue
		app.Spec.Executor.Labels[QueueLabel] = *app.Spec.BatchSchedulerOptions.Queue
	}
}

func New(config *rest.Config) (schedulerinterface.BatchScheduler, error) {
	return &YunikornScheduler{}, nil
}
