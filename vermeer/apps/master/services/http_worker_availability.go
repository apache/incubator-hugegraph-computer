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
	"net/http"
	"vermeer/apps/common"

	"github.com/gin-gonic/gin"
)

// WorkerAvailabilityHandler reports readiness only for the authenticated space.
type WorkerAvailabilityHandler struct {
	common.SenHandler
}

func (wh *WorkerAvailabilityHandler) GET(ctx *gin.Context) {
	writeWorkerAvailability(ctx, WorkerMgr.HasAvailableWorkers)
}

func writeWorkerAvailability(ctx *gin.Context, available func(string, []string) bool) {
	space := credential(ctx).Space()
	ctx.JSON(http.StatusOK, gin.H{"available": available(space, availabilityGraphNames(space))})
}

func availabilityGraphNames(space string) []string {
	// Match AddGraph's publication lock before GetGraphs takes the space lock.
	// Names are immutable; do not read the graph's changing Workers slice.
	defer GraphMgr.Unlock(GraphMgr.Lock())
	graphs := GraphMgr.GetGraphs(space)
	names := make([]string, 0, len(graphs))
	for _, graph := range graphs {
		names = append(names, graph.Name)
	}
	return names
}
