package webhook

import (
	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"maps"
)

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
	Name         string                                    `json:"name"`
	MinMember    int32                                     `json:"minMember"`
	MinResource  map[corev1.ResourceName]resource.Quantity `json:"minResource,omitempty"`
	NodeSelector map[string]string                         `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration                       `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity                          `json:"affinity,omitempty"`
	Labels       map[string]string                         `json:"labels,omitempty"`
}

func getInitialExecutors(app *v1beta2.SparkApplication) int32 {
	// Take the max of the number of executors and both the initial and minimum number of executors from
	// dynamic allocation. See the upstream Spark code below for reference.
	// https://github.com/apache/spark/blob/bc187013da821eba0ffff2408991e8ec6d2749fe/core/src/main/scala/org/apache/spark/util/Utils.scala#L2539-L2542
	initialExecutors := int32(0)

	if app.Spec.Executor.Instances != nil {
		initialExecutors = max(initialExecutors, *app.Spec.Executor.Instances)
	}

	if app.Spec.DynamicAllocation != nil {
		if app.Spec.DynamicAllocation.MinExecutors != nil {
			initialExecutors = max(initialExecutors, *app.Spec.DynamicAllocation.MinExecutors)
		}
		if app.Spec.DynamicAllocation.InitialExecutors != nil {
			initialExecutors = max(initialExecutors, *app.Spec.DynamicAllocation.InitialExecutors)
		}
	}

	return initialExecutors
}

func mergeMaps(m1, m2 map[string]string) map[string]string {
	out := make(map[string]string)

	maps.Copy(out, m1)
	maps.Copy(out, m2)

	// Return nil if there are no keys so the struct field is skipped JSON marshalling
	if len(out) == 0 {
		return nil
	}
	return out
}
