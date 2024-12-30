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

package services

import (
	"vermeer/apps/common"
	. "vermeer/apps/master/bl"
	pb "vermeer/apps/protos"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

func SetRouters(sen *common.Sentinel, authFilters ...gin.HandlerFunc) {
	// No authentication
	regVerAPI(sen, 1, "", map[string]common.BaseHandler{
		"/healthcheck": &common.HelloHandler{},
		"/login":       &LoginHandler{},
	})

	// /tasks
	regVerAPI(sen, 1, "/tasks", map[string]common.BaseHandler{
		"":        &TasksHandler{},
		"/create": &TaskCreateHandler{},
		// "/create/batch":   &TaskCreateBatchHandler{},
		"/create/sync":    &TaskCreateSyncHandler{},
		"/oltp":           &OltpHandler{},
		"/value/:task_id": &ComputeValueHandler{},
	}, authFilters...)

	// /task
	regVerAPI(sen, 1, "/task", map[string]common.BaseHandler{
		"/:task_id":        &TaskHandler{},
		"/cancel/:task_id": &TaskCancelHandler{},
	}, authFilters...)

	// /graphs
	regVerAPI(sen, 1, "/graphs", map[string]common.BaseHandler{
		"":                      &GraphsHandler{},
		"/create":               &GraphCreateHandler{},
		"/:graph_name":          &GraphHandler{},
		"/:graph_name/edges":    &EdgesHandler{},
		"/:graph_name/vertices": &VerticesHandler{},
	}, authFilters...)

	// /workers
	regVerAPI(sen, 1, "/workers", map[string]common.BaseHandler{
		"": &WorkersHandler{},
	})

	// /master
	regVerAPI(sen, 1, "/master", map[string]common.BaseHandler{
		"": &InfoMasterHandler{},
	})

}

func SetAdminRouters(sen *common.Sentinel, authFilters ...gin.HandlerFunc) {
	// /admin
	regVerAPI(sen, 1, "/admin", map[string]common.BaseHandler{
		"/graphs/:space_name":                      &AdminGraphsHandler{},
		"/graphs/:space_name/create":               &AdminGraphCreateHandler{},
		"/graphs/:space_name/:graph_name":          &AdminGraphHandler{},
		"/graphs/:space_name/:graph_name/edges":    &AdminEdgesHandler{},
		"/graphs/:space_name/:graph_name/vertices": &AdminVerticesHandler{},
		"/graphs/:space_name/:graph_name/save":     &AdminGraphSaveHandler{},
		"/graphs/:space_name/:graph_name/read":     &AdminGraphReadHandler{},

		"/workers":                                             &AdminWorkersHandler{},
		"/workers/alloc/:worker_group":                         &AdminWorkerGroupAllocHandler{},
		"/workers/alloc/:worker_group/:space_name":             &AdminWorkerGroupSpaceAllocHandler{},
		"/workers/alloc/:worker_group/:space_name/:graph_name": &AdminWorkerGroupGraphAllocHandler{},
		"/workers/group/:worker_group/:worker_name":            &AdminWorkerGroupHandler{},

		"/scheduler/dispatch/pause":  &AdminDispatchPauseHandler{},
		"/scheduler/dispatch/resume": &AdminDispatchResumeHandler{},

		"/threshold/memory": &AdminThresholdMemHandler{},
	}, authFilters...)
}

func SetUI(sen *common.Sentinel, authFilters ...gin.HandlerFunc) {
	// No authentication
	sen.Register(map[string]common.BaseHandler{
		"/": &MasterUIHandler{},
	})
}

func regVerAPI(sen *common.Sentinel, verion int, base string, routers map[string]common.BaseHandler, filters ...gin.HandlerFunc) {
	// for compatibility with old vermeer version
	if verion == 1 {
		sen.RegisterGroup(base, routers, filters...)
	}
	sen.RegisterAPI(verion, base, routers, filters...)
}

// TODO: add dashboard router

func GrpcRegister(s grpc.ServiceRegistrar) {
	sh := ServerHandler{}
	sh.Init()
	pb.RegisterMasterServer(s, &sh)
}

//func PeriodRouter() map[string]common.PeriodHandler {
//	router := map[string]common.PeriodHandler{}
//	return router
//}
