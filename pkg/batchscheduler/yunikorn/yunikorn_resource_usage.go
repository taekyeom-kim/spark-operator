package yunikorn

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	defaultCpuMillicores = resource.NewMilliQuantity(1000, resource.DecimalSI)

	defaultMemoryBytes = resource.NewQuantity(int64(1<<30), resource.BinarySI)
	// https://github.com/apache/spark/blob/c4bbfd177b4e7cb46f47b39df9fd71d2d9a12c6d/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Constants.scala#L85
	minMemoryOverhead           = resource.NewQuantity(384*(1<<20), resource.BinarySI) // 384Mi
	defaultMemoryOverhead       = 0.1
	nonJvmDefaultMemoryOverhead = 0.4

	emptyResourceMap = map[corev1.ResourceName]string{}
)

// TODO (taekyeom.kim) make it common...
// borrowed from "github.com/kubeflow/spark-operator/pkg/webhook/resourceusage"
func coresRequiredForSparkPod(spec v1beta2.SparkPodSpec) (*resource.Quantity, error) {
	var cpu *resource.Quantity
	if spec.Cores != nil {
		cpu = resource.NewMilliQuantity(int64(*spec.Cores)*1000, resource.BinarySI)
	} else {
		cpu = defaultCpuMillicores
	}
	return cpu, nil
}

var javaMemorySuffixToScale = map[string]string{
	"b":  "",
	"kb": "Ki",
	"k":  "Ki",
	"mb": "Mi",
	"m":  "Mi",
	"gb": "Gi",
	"g":  "Gi",
	"tb": "Ti",
	"t":  "Ti",
	"pb": "Pi",
	"p":  "Pi",
}

var javaStringPattern = regexp.MustCompile(`([0-9]+)([a-z]+)?`)
var javaFractionStringPattern = regexp.MustCompile(`([0-9]+\.[0-9]+)([a-z]+)?`)

func parseJavaMemoryString(str string) (resource.Quantity, error) {
	lower := strings.ToLower(str)
	if matches := javaStringPattern.FindStringSubmatch(lower); matches != nil {
		value := matches[1]
		suffix := matches[2]
		if scale, present := javaMemorySuffixToScale[suffix]; present {
			return resource.ParseQuantity(fmt.Sprintf("%s%s", value, scale))

		}
	} else if matches = javaFractionStringPattern.FindStringSubmatch(lower); matches != nil {
		value := matches[1]
		suffix := matches[2]
		if scale, present := javaMemorySuffixToScale[suffix]; present {
			return resource.ParseQuantity(fmt.Sprintf("%s%s", value, scale))

		}
	}
	return resource.Quantity{}, fmt.Errorf("could not parse string '%s' as a Java-style memory value. Examples: 100kb, 1.5mb, 1g", str)
}

// Logic copied from https://github.com/apache/spark/blob/c4bbfd177b4e7cb46f47b39df9fd71d2d9a12c6d/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicDriverFeatureStep.scala
func memoryRequiredForSparkPod(spec v1beta2.SparkPodSpec, memoryOverheadFactor *string, appType v1beta2.SparkApplicationType) (*resource.Quantity, error) {
	var memoryBytes *resource.Quantity
	if spec.Memory != nil {
		memory, err := parseJavaMemoryString(*spec.Memory)
		if err != nil {
			return nil, err
		}
		memoryBytes = &memory
	} else {
		memoryBytes = defaultMemoryBytes
	}
	var memoryOverheadBytes resource.Quantity
	if spec.MemoryOverhead != nil {
		overhead, err := parseJavaMemoryString(*spec.MemoryOverhead)
		if err != nil {
			return nil, err
		}
		memoryOverheadBytes = overhead
	} else {
		var overheadFactor float64
		if memoryOverheadFactor != nil {
			overheadFactorScope, err := strconv.ParseFloat(*memoryOverheadFactor, 64)
			if err != nil {
				return nil, err
			}
			overheadFactor = overheadFactorScope
		} else {
			if appType == v1beta2.JavaApplicationType {
				overheadFactor = defaultMemoryOverhead
			} else {
				overheadFactor = nonJvmDefaultMemoryOverhead
			}
		}
		memoryOverheadBytes = *resource.NewQuantity(int64(float64(memoryBytes.Value())*overheadFactor), resource.BinarySI)
		if minMemoryOverhead.Cmp(memoryOverheadBytes) == 1 {
			memoryOverheadBytes = *minMemoryOverhead
		}
	}
	memoryBytes.Add(memoryOverheadBytes)
	return memoryBytes, nil
}

func cpuRequiredForSidecar(sidecar corev1.Container) resource.Quantity {
	requestCpu, ok := sidecar.Resources.Requests[corev1.ResourceCPU]
	if !ok {
		return resource.Quantity{}
	}
	return requestCpu
}

func memoryRequiredForSidecar(sidecar corev1.Container) resource.Quantity {
	requestMemory, ok := sidecar.Resources.Requests[corev1.ResourceMemory]
	if !ok {
		return resource.Quantity{}
	}
	return requestMemory
}

func driverPodResourceUsage(app *v1beta2.SparkApplication) (map[corev1.ResourceName]string, error) {
	driverMemoryOverheadFactor := app.Spec.MemoryOverheadFactor
	driverMemory, err := memoryRequiredForSparkPod(app.Spec.Driver.SparkPodSpec, driverMemoryOverheadFactor, app.Spec.Type)
	if err != nil {
		return emptyResourceMap, err
	}

	driverCores, err := coresRequiredForSparkPod(app.Spec.Driver.SparkPodSpec)
	if err != nil {
		return emptyResourceMap, err
	}

	for _, sidecar := range app.Spec.Driver.Sidecars {
		driverCores.Add(cpuRequiredForSidecar(sidecar))
		driverMemory.Add(memoryRequiredForSidecar(sidecar))
	}

	return map[corev1.ResourceName]string{
		corev1.ResourceCPU:    driverCores.String(),
		corev1.ResourceMemory: driverMemory.String(),
	}, nil
}

func executorPodResourceUsage(app *v1beta2.SparkApplication) (map[corev1.ResourceName]string, error) {
	executorMemoryOverheadFactor := app.Spec.MemoryOverheadFactor
	executorMemory, err := memoryRequiredForSparkPod(app.Spec.Executor.SparkPodSpec, executorMemoryOverheadFactor, app.Spec.Type)
	if err != nil {
		return emptyResourceMap, err
	}

	executorCores, err := coresRequiredForSparkPod(app.Spec.Executor.SparkPodSpec)
	if err != nil {
		return emptyResourceMap, err
	}

	for _, sidecar := range app.Spec.Executor.Sidecars {
		executorCores.Add(cpuRequiredForSidecar(sidecar))
		executorMemory.Add(memoryRequiredForSidecar(sidecar))
	}

	return map[corev1.ResourceName]string{
		corev1.ResourceCPU:    executorCores.String(),
		corev1.ResourceMemory: executorMemory.String(),
	}, nil
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
