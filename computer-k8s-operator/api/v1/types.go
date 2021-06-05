/*
Copyright 2017 HugeGraph Authors

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.
// Any new fields you add must have json tags for the fields to be serialized.

// ComputerJobSpec defines the desired state of HugeGraphComputerJob
type ComputerJobSpec struct {
	AlgorithmName *string `json:"algorithmName,omitempty"`

	JobId *string `json:"jobId,omitempty"`

	Image *string `json:"image,omitempty"`

	//+kubebuilder:validation:Minimum=1
	WorkerInstances *int32 `json:"workerInstances,omitempty"`

	MasterCpu *string `json:"masterCpu,omitempty"`

	WorkerCpu *int32 `json:"workerCpu,omitempty"`

	MasterMemory *string `json:"masterMemory,omitempty"`

	WorkerMemory *string `json:"workerMemory,omitempty"`

	ComputerConf map[string]string `json:"computerConf,omitempty"`

	ConfigMap *string `json:"configMap,omitempty"`

	// Environment variables shared by all JobManager, TaskManager and job
	// containers.
	EnvVars []corev1.EnvVar `json:"envVars,omitempty"`

	// Environment variables injected from a source, shared by all JobManager,
	// TaskManager and job containers.
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`
}

type ComputerJobState struct {
	Superstep *int32 `json:"superstep,omitempty"`

	MaxSuperstep *int32 `json:"maxSuperstep,omitempty"`

	LastSuperstepStat *string `json:"lastSuperstepStat,omitempty"`
}

// ComputerJobStatus defines the observed state of HugeGraphComputerJob
type ComputerJobStatus struct {
	JobStatus *string `json:"jobStatus,omitempty"`

	JobState *ComputerJobState `json:"jobState,omitempty"`

	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=".spec.jobId",name=JobId,type=string
// +kubebuilder:printcolumn:JSONPath=".status.jobStatus",name=JobStatus,type=string
// +kubebuilder:printcolumn:JSONPath=".status.jobState.superstep",name=Superstep,type=integer
// +kubebuilder:printcolumn:JSONPath=".status.jobState.maxSuperstep",name=JobStatus,type=integer
// +kubebuilder:printcolumn:JSONPath=".status.jobState.lastSuperstepStat",name=SuperstepStat,type=string

// HugeGraphComputerJob is the Schema for the hugegraphcomputerjobs API
type HugeGraphComputerJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComputerJobSpec   `json:"spec,omitempty"`
	Status ComputerJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HugeGraphComputerJobList contains a list of HugeGraphComputerJob
type HugeGraphComputerJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []HugeGraphComputerJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HugeGraphComputerJob{}, &HugeGraphComputerJobList{})
}
