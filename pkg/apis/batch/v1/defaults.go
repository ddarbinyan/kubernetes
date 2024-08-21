/*
Copyright 2016 The Kubernetes Authors.

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

package v1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	utilpointer "k8s.io/utils/pointer"
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

func SetDefaults_Job(obj *batchv1.Job) {
	// For a non-parallel job, you can leave both `.spec.completions` and
	// `.spec.parallelism` unset.  When both are unset, both are defaulted to 1.
	if obj.ObjectMeta.UID == "" {
		obj.ObjectMeta.UID = uuid.NewUUID()
		obj.ObjectMeta.CreationTimestamp = metav1.Now()
	}

	if obj.Spec.Completions == nil && obj.Spec.Parallelism == nil {
		klog.Infof("Updating obj.Spec.Parallelism to 1")
		obj.Spec.Completions = utilpointer.Int32(1)
		obj.Spec.Parallelism = utilpointer.Int32(1)
	}
	if obj.Spec.Parallelism == nil {
		klog.Infof("Updating obj.Spec.Parallelism to 1")
		obj.Spec.Parallelism = utilpointer.Int32(1)
	}
	if obj.Spec.BackoffLimit == nil {
		klog.Infof("Updating obj.Spec.BackoffLimit to 6")
		obj.Spec.BackoffLimit = utilpointer.Int32(6)
	}
	labels := obj.Spec.Template.Labels
	if labels != nil && len(obj.Labels) == 0 {
		klog.Infof("Updating obj.Labels")
		obj.Labels = labels
	}
	if obj.Spec.CompletionMode == nil {
		klog.Infof("Updating obj.Spec.CompletionMode")
		mode := batchv1.NonIndexedCompletion
		obj.Spec.CompletionMode = &mode
	}
	if obj.Spec.Suspend == nil {
		klog.Infof("Updating obj.Spec.Suspend to false")
		obj.Spec.Suspend = utilpointer.Bool(false)
	}
	if obj.Spec.PodFailurePolicy != nil {
		for _, rule := range obj.Spec.PodFailurePolicy.Rules {
			if rule.OnPodConditions != nil {
				for i, pattern := range rule.OnPodConditions {
					if pattern.Status == "" {
						klog.Infof("Updating obj.Spec.PodFailurePolicy.Rules.OnPodConditions.Status to corev1.ConditionTrue")
						rule.OnPodConditions[i].Status = corev1.ConditionTrue
					}
				}
			}
		}
	}
	if obj.Spec.ManualSelector == nil || !*obj.Spec.ManualSelector {
		if(generateSelector(obj)){
			klog.Infof("Updating through generateSelector()")
		}
	}
}

// generateSelector adds a selector to a job and labels to its template
// which can be used to uniquely identify the pods created by that job,
// if the user has requested this behavior.
func generateSelector(obj *batchv1.Job) bool {
	updated := false

	if obj.Spec.Template.Labels == nil {
		obj.Spec.Template.Labels = make(map[string]string)
		updated = true
	}
	// The job-name label is unique except in cases that are expected to be
	// quite uncommon, and is more user friendly than uid.  So, we add it as
	// a label.
	jobNameLabels := []string{batchv1.JobNameLabel}
	for _, value := range jobNameLabels {
		_, found := obj.Spec.Template.Labels[value]
		if found {
			// User asked us to automatically generate a selector, but set manual labels.
			// If there is a conflict, we will reject in validation.
		} else {
			obj.Spec.Template.Labels[value] = string(obj.ObjectMeta.Name)
			updated = true
		}
	}

	// The controller-uid label makes the pods that belong to this job
	// only match this job.
	controllerUidLabels := []string{batchv1.ControllerUidLabel}
	for _, value := range controllerUidLabels {
		_, found := obj.Spec.Template.Labels[value]
		if found {
			// User asked us to automatically generate a selector, but set manual labels.
			// If there is a conflict, we will reject in validation.
		} else {
			obj.Spec.Template.Labels[value] = string(obj.ObjectMeta.UID)
			updated = true
		}
	}
	// Select the controller-uid label.  This is sufficient for uniqueness.
	if obj.Spec.Selector == nil {
		obj.Spec.Selector = &metav1.LabelSelector{}
		updated = true
	}
	if obj.Spec.Selector.MatchLabels == nil {
		obj.Spec.Selector.MatchLabels = make(map[string]string)
		updated = true
	}

	if _, found := obj.Spec.Selector.MatchLabels[batchv1.ControllerUidLabel]; !found {
		obj.Spec.Selector.MatchLabels[batchv1.ControllerUidLabel] = string(obj.ObjectMeta.UID)
		updated = true
	}
	// If the user specified matchLabel controller-uid=$WRONGUID, then it should fail
	// in validation, either because the selector does not match the pod template
	// (controller-uid=$WRONGUID does not match controller-uid=$UID, which we applied
	// above, or we will reject in validation because the template has the wrong
	// labels.

	return updated
}

func SetDefaults_CronJob(obj *batchv1.CronJob) {
	if obj.Spec.ConcurrencyPolicy == "" {
		obj.Spec.ConcurrencyPolicy = batchv1.AllowConcurrent
	}
	if obj.Spec.Suspend == nil {
		obj.Spec.Suspend = utilpointer.Bool(false)
	}
	if obj.Spec.SuccessfulJobsHistoryLimit == nil {
		obj.Spec.SuccessfulJobsHistoryLimit = utilpointer.Int32(3)
	}
	if obj.Spec.FailedJobsHistoryLimit == nil {
		obj.Spec.FailedJobsHistoryLimit = utilpointer.Int32(1)
	}
}
