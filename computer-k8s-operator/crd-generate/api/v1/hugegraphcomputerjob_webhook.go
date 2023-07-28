/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package v1

import (
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"strings"
)

// log is for logging in this package.
var hugegraphcomputerjoblog = logf.Log.WithName("hugegraphcomputerjob-resource")

func (r *HugeGraphComputerJob) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-operator-hugegraph-apache-org-v1-hugegraphcomputerjob,mutating=true,failurePolicy=fail,sideEffects=None,groups=operator.hugegraph.apache.org,resources=hugegraphcomputerjobs,verbs=create;update,versions=v1,name=mhugegraphcomputerjob.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HugeGraphComputerJob{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *HugeGraphComputerJob) Default() {
	hugegraphcomputerjoblog.Info("default", "name", r.Name)
}

//+kubebuilder:webhook:path=/validate-operator-hugegraph-apache-org-v1-hugegraphcomputerjob,mutating=false,failurePolicy=fail,sideEffects=None,groups=operator.hugegraph.apache.org,resources=hugegraphcomputerjobs,verbs=create;update,versions=v1,name=vhugegraphcomputerjob.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HugeGraphComputerJob{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HugeGraphComputerJob) ValidateCreate() error {
	hugegraphcomputerjoblog.Info("validate create", "name", r.Name)

	return nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HugeGraphComputerJob) ValidateUpdate(old runtime.Object) error {
	hugegraphcomputerjoblog.Info("validate update", "name", r.Name)
	var allErrs field.ErrorList

	flag := false

	fmt.Println(r.Status.JobStatus)

	if r.Status.JobStatus == nil {
		return nil
	}
	if strings.EqualFold(*r.Status.JobStatus, "RUNNING") ||
		strings.EqualFold(*r.Status.JobStatus, "INITIALIZING") {
		flag = true
	}
	if flag == true {
		hugegraphcomputerjoblog.Info("Status NULL, can not update", "name", r.Name)
		err := field.Invalid(field.ToPath(),
			r.Name,
			"Current HugeGraphComputerJob is running, can not update")

		allErrs = append(allErrs, err)

		return apierrors.NewInvalid(
			schema.GroupKind{
				Group: "hugegraph",
				Kind:  "HugeGraphComputerJob",
			},
			r.Name,
			allErrs)
	}

	hugegraphcomputerjoblog.Info("status is not null")
	return nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HugeGraphComputerJob) ValidateDelete() error {
	hugegraphcomputerjoblog.Info("validate delete", "name", r.Name)

	return nil
}
