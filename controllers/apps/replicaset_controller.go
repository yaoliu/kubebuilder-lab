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
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-logr/logr"
	appsv1 "github.com/yaoliu/kubebuilder-lab/apis/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
)

// ReplicaSetReconciler reconciles a ReplicaSet object
type ReplicaSetReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.liuyao.me,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.liuyao.me,resources=replicasets/status,verbs=get;update;patch

func (r *ReplicaSetReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("replicaset", req.NamespacedName)

	rs := &appsv1.ReplicaSet{}

	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		log.Error(err, "unable to fetch ReplicaSet")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	podList := &corev1.PodList{}
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
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
		if controllerRef != nil && controllerRef.Kind == rs.Kind && controllerRef.UID == rs.UID {
			pods = append(pods, pod)
		}
	}
	activePods := filterActivePods(pods)
	var reconcilePodsErr error

	if rs.DeletionTimestamp == nil {
		reconcilePodsErr = r.reconcilePods(activePods, rs)
	}
	// why deepcopy
	rs = rs.DeepCopy()

	//newStatus := calculateStatus(rs, activePods, reconcilePodsErr)

	//newRs := func() *appsv1.ReplicaSet {
	//	return nil
	//}
	//
	//if manageReplicasErr == nil &&
	//	rs.Spec.

	return ctrl.Result{}, nil
}

func (r *ReplicaSetReconciler) reconcilePods(activePods []*corev1.Pod, rs *appsv1.ReplicaSet) error {
	diff := len(activePods) - int(*(rs.Spec.Replicas))
	if diff < 0 {
		diff *= -1

	}
	return nil
}

func filterActivePods(pods []*corev1.Pod) (active []*corev1.Pod) {
	for _, pod := range pods {
		if isPodActive(pod) {
			active = append(active, pod)
		} else {
		}
	}
	return active
}

func isPodActive(p *corev1.Pod) bool {
	return corev1.PodSucceeded != p.Status.Phase &&
		corev1.PodFailed != p.Status.Phase &&
		p.DeletionTimestamp == nil
}

func calculateReplicaSetStatus(rs *appsv1.ReplicaSet, activePods []*corev1.Pod, reconcilePodsErr error) appsv1.ReplicaSetStatus {

	fullyLabeledReplicasCount := 0
	readyReplicasCount := 0
	availableReplicas := 0

	templateLabels := labels.Set(rs.Spec.Template.Labels).AsSelectorPreValidated()

	for _, pod := range activePods {
		if templateLabels.Matches(labels.Set(pod.Labels)) {
			fullyLabeledReplicasCount += 1
		}
		if podutil.I

	}

	ready
}

func (r *ReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.ReplicaSet{}).
		Complete(r)
}
