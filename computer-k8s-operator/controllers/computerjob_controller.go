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

package controllers

import (
	"context"

	computerv1 "computer.hugegraph.io/operator/api/v1"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ComputerJobReconciler reconciles a HugeGraphComputerJob object
type ComputerJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
	Mgr    ctrl.Manager
}

// +kubebuilder:rbac:groups=computer.hugegraph.io,resources=hugegraphcomputerjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=computer.hugegraph.io,resources=hugegraphcomputerjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=computer.hugegraph.io,resources=hugegraphcomputerjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=extensions,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensions,resources=ingresses/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the HugeGraphComputerJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
func (reconciler *ComputerJobReconciler) Reconcile(
	ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := reconciler.Log.WithValues(ctx, "ComputerJob", req.NamespacedName)
	log.Info("reconclice request", "request", req)

	var computerJob *computerv1.HugeGraphComputerJob
	if err := reconciler.Get(ctx, req.NamespacedName, computerJob); err != nil {
		log.Error(err, "Unable to fetch ComputerJob")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// TODO: Modify the Reconcile function to compare the state
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (reconciler *ComputerJobReconciler) SetupWithManager(
	mgr ctrl.Manager) error {
	reconciler.Mgr = mgr
	return ctrl.NewControllerManagedBy(mgr).
		For(&computerv1.HugeGraphComputerJob{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Event{}).
		Complete(reconciler)
}

// ComputerJobHandler holds the context and state for a reconcile request.
type ComputerJobHandler struct {
	k8sClient client.Client
	request   ctrl.Request
	context   context.Context
	log       logr.Logger
	recorder  record.EventRecorder
}
