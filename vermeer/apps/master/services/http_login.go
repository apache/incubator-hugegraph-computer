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
	"vermeer/apps/auth"
	"vermeer/apps/common"

	"github.com/gin-gonic/gin"
)

//const cookieTokenExpire = 1 * time.Minute

type LoginRequest struct {
	Token *string
}

type LoginHandler struct {
	common.SenHandler
}

func (l *LoginHandler) POST(ctx *gin.Context) {
	req := LoginRequest{}
	err := ctx.BindJSON(&req)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}

	if req.Token != nil {
		doTokenLogin(ctx, &req)
		return
	}

	if isBad(true, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}
}

func doTokenLogin(ctx *gin.Context, req *LoginRequest) {
	token := auth.ToTokenBase58(*req.Token)
	if isBad(token == nil, ctx, func() string { return "invalid token string format" }) {
		return
	}

	setTokenCookie(ctx, *req.Token)

	ok(ctx, "login success")
}

func setTokenCookie(ctx *gin.Context, token string) {
	// expires: session
	ctx.SetCookie(auth.CookieKey, token, 0, "/", "", false, true)
}
