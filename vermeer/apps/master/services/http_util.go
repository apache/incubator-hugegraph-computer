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
	"fmt"
	"net/http"
	"vermeer/apps/common"
	. "vermeer/apps/master/bl"
	. "vermeer/apps/master/graphs"
	"vermeer/apps/structure"

	"github.com/gin-gonic/gin"
)

func credential(ctx *gin.Context) *structure.Credential {
	if v, ok := ctx.Get(structure.CredentialKey); ok {
		if c, ok := v.(*structure.Credential); ok {
			return c
		}
	}

	ctx.JSON(http.StatusInternalServerError, gin.H{
		"errcode": -1,
		"message": "Failed to retrieve any credential.",
	})
	panic("Unauthorized access.")
}

func adminCred(ctx *gin.Context, spaceName string) *structure.Credential {
	cred := credential(ctx)
	if cred.IsAdmin() {
		return structure.NewAdminCred(spaceName)
	}
	ctx.JSON(http.StatusInternalServerError, gin.H{
		"errcode": -1,
		"message": "Failed to retrieve the admin credential.",
	})
	panic("Unauthorized access.")
}

func isErr(err error, ctx *gin.Context) bool {
	return isBad(err != nil, ctx, func() string { return err.Error() })
}

func isBadReq(err error, ctx *gin.Context) bool {
	return isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) })
}

func isBad(predicate bool, ctx *gin.Context, msg Supplier[string]) (bad bool) {
	if predicate {
		ctx.JSON(http.StatusBadRequest, common.BaseResp{ErrCode: 1, Message: msg()})
		return true
	}
	return false
}

func ok(ctx *gin.Context, msg string) {
	ctx.JSON(http.StatusOK, common.BaseResp{ErrCode: 0, Message: msg})
}

func taskBiz(ctx *gin.Context) *TaskBl {
	return &TaskBl{Cred: credential(ctx)}
}

func graphBiz(ctx *gin.Context) *GraphBl {
	return &GraphBl{Cred: credential(ctx)}
}

func adminBiz(ctx *gin.Context) *AdminBl {
	return &AdminBl{Cred: credential(ctx)}
}

func adminGraphBiz(ctx *gin.Context, space string) *GraphBl {
	return &GraphBl{Cred: adminCred(ctx, space)}
}

func toSpaceGraph(space, graph string) string {
	return fmt.Sprintf("%s/%s", space, graph)
}
