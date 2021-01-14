/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	batchv1 "github.com/yaoliu/kubebuilder-lab/apis/batch/v1"
)

// JobReconciler reconciles a Job object
type JobReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=batch.liuyao.me,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.liuyao.me,resources=jobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;update;patch

func (r *JobReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("job", req.NamespacedName)

	// 1. 获取Job资源对象
	job := &batchv1.Job{}

	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		log.Error(err, "unable to fetch Job")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. 判断是否已经运行完毕
	if isJobFinished(job) {
		return ctrl.Result{}, nil
	}
	// 3. 获取所关联的Job
	podList := &corev1.PodList{}
	selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
	if err != nil {
		return ctrl.Result{}, err
	}
	listOptions := &client.ListOptions{
		Namespace:     req.Namespace,
		LabelSelector: selector,
	}
	if err := r.List(ctx, podList, listOptions); err != nil {
		return ctrl.Result{}, err
	}
	var pods []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef != nil && controllerRef.Kind == job.Kind && controllerRef.UID == job.UID {
			pods = append(pods, pod)
		}
	}
	// 4. 将pod进行分类
	activePods, failedPods, _ := filterPods(pods)
	active := int32(len(activePods))
	failed := int32(len(failedPods))
	//succeeded := int32(len(succeededPods))

	// 5. 判断job资源对象是否是第一次运行 如果是第一次运行 需要为它设置开始时间
	requeueAfter := time.Duration(0)
	if job.Status.StartTime == nil {
		now := metav1.Now()
		job.Status.StartTime = &now
		// 6. 判断是否配置ActiveDeadlineSeconds
		if job.Spec.ActiveDeadlineSeconds != nil {
			requeueAfter = time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second
		}
	}
	// 7. 获取当前Job状态
	jobFailed := false
	var failureReason string
	var failureMessage string
	// spec.status.failed = pod失败数量
	//jobHaveNewFailure := failed > job.Status.Failed
	//// spec.parallelism 并发执行pod数量 是否和active相等
	//jobHaveActive := active != *job.Spec.Parallelism
	//previousRetry := 0

	if pastBackoffLimitOnFailure(job, pods) {
		jobFailed = true
		failureReason = "BackoffLimitExceeded"
		failureMessage = "Job has reached the specified backoff limit"
	} else if pastActiveDeadline(job) {
		jobFailed = true
		failureReason = "DeadlineExceeded"
		failureMessage = "Job was active longer than specified deadline"
	}
	// 8. 如果job资源对象处于failed状态 那么需要删除所有pod
	if jobFailed {
		r.deleteJobPods(job, activePods)
		failed += active
		active = 0
		job.Status.Conditions = append(job.Status.Conditions, newCondition(batchv1.JobFailed, failureReason, failureMessage))
		r.Recorder.Event(job, corev1.EventTypeWarning, failureReason, failureMessage)

	} else {
		// 判断是否需要同步 比如创建Pod
		if job.DeletionTimestamp == nil {

		}
	}

	// end 更新状态
	//if job.Status.Active != active || job.Status.Failed

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *JobReconciler) deleteJobPods(job *batchv1.Job, pods []*corev1.Pod) {
	wg := sync.WaitGroup{}
	podsLen := len(pods)
	wg.Add(podsLen)
	for i := int32(0); i < int32(podsLen); i++ {
		go func(index int32) {
			defer wg.Done()
			pod := pods[index]
			if err := r.Delete(context.TODO(), pod); err != nil {

			}
		}(i)
	}
	wg.Wait()
}

func isJobFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func filterPods(pods []*corev1.Pod) (active, successed, failed []*corev1.Pod) {
	for _, pod := range pods {
		if corev1.PodSucceeded != pod.Status.Phase && corev1.PodFailed != pod.Status.Phase && pod.DeletionTimestamp == nil {
			active = append(active, pod)
		}
		if corev1.PodSucceeded == pod.Status.Phase {
			successed = append(successed, pod)
		}
		if corev1.PodFailed == pod.Status.Phase {
			failed = append(failed, pod)
		}
	}
	return active, successed, failed
}

func pastActiveDeadline(job *batchv1.Job) bool {
	if job.Spec.ActiveDeadlineSeconds == nil || job.Status.StartTime == nil {
		return false
	}
	now := metav1.Now()
	start := job.Status.StartTime.Time
	duration := now.Time.Sub(start)
	allowedDuration := time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}

func pastBackoffLimitOnFailure(job *batchv1.Job, pods []*corev1.Pod) bool {
	// 当容器终止运行且退出码不为0时，由kubelet自动重启该容器
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyOnFailure {
		return false
	}
	result := int32(0)
	for i := range pods {
		pod := pods[i]
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			for j := range pod.Status.InitContainerStatuses {
				stat := pod.Status.InitContainerStatuses[j]
				result = stat.RestartCount
			}
			for j := range pod.Status.ContainerStatuses {
				stat := pod.Status.ContainerStatuses[j]
				result = stat.RestartCount
			}
		}
	}
	if *job.Spec.BackoffLimit == 0 {
		return result > 0
	}

	return result > *job.Spec.BackoffLimit
}

func newCondition(conditionType batchv1.JobConditionType, reason, message string) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		LastProbeTime:      metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}).
		Complete(r)
}
