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

package common

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type BaseHandler interface {
	GET(ctx *gin.Context)
	POST(ctx *gin.Context)
	PUT(ctx *gin.Context)
	DELETE(ctx *gin.Context)
}

type SenHandler struct {
	BaseHandler
}

func (s *SenHandler) GET(ctx *gin.Context) {
	logrus.Errorf("method not implement")
	ctx.JSON(http.StatusBadRequest, BaseResp{ErrCode: -1, Message: "method not implement"})
}

func (s *SenHandler) POST(ctx *gin.Context) {
	logrus.Errorf("method not implement")
	ctx.JSON(http.StatusBadRequest, BaseResp{ErrCode: -1, Message: "method not implement"})
}

func (s *SenHandler) PUT(ctx *gin.Context) {
	logrus.Errorf("method not implement")
	ctx.JSON(http.StatusBadRequest, BaseResp{ErrCode: -1, Message: "method not implement"})
}

func (s *SenHandler) DELETE(ctx *gin.Context) {
	logrus.Errorf("method not implement")
	ctx.JSON(http.StatusBadRequest, BaseResp{ErrCode: -1, Message: "method not implement"})
}

type PeriodHandler interface {
	Init() error
	Process()
}

type PreprocessHandler interface {
	Init() error
	Process()
}

type BaseResp struct {
	ErrCode int32  `json:"errcode"`
	Message string `json:"message,omitempty"`
}

type SenContext gin.Context

func (ctx *SenContext) GetParam(key string, defValue string) string {
	if values, ok := ctx.Request.Form[key]; ok {
		if len(values) > 0 {
			return values[0]
		}
	}
	return defValue
}

func (ctx *SenContext) GetParamInt64(key string, defValue int64) int64 {
	value, err := strconv.ParseInt(ctx.GetParam(key, ""), 10, 64)
	if err != nil {
		return defValue
	}
	return value
}

func (ctx *SenContext) GetParamFloat(key string, defValue float64) float64 {
	value, err := strconv.ParseFloat(ctx.GetParam(key, ""), 64)
	if err != nil {
		return defValue
	}
	return value
}

func LoggerHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		startTime := time.Now()
		ctx.Next()
		endTime := time.Now()
		latencyTime := float32(endTime.Sub(startTime)) / 1000000
		reqMethod := ctx.Request.Method
		reqUri := ctx.Request.URL.Path
		statusCode := ctx.Writer.Status()
		extInfo := ctx.GetString("log_ext_info")
		if latencyTime > 200 {
			logrus.Warnf("slow request: %.1fms, uri: %s", latencyTime, reqUri)
		}
		logrus.Infof("%3d %.1fms %s %s %s %s",
			statusCode,
			latencyTime,
			ctx.ClientIP(),
			reqMethod,
			reqUri,
			extInfo,
		)
	}
}

func ErrorHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Next()
		if ctx.Writer.Status() < http.StatusInternalServerError {
			return
		}
		//buf := make([]byte, 1<<16)
		//s := runtime.Stack(buf, false)
		//dingUrl := GetConfigDefault("dingding_url", "").(string)
		//lastDingTime := GetConfigDefault("last_ding_time", time.Unix(1085117489, 0)).(time.Time)
		//if dingUrl != "" && time.Now().Sub(lastDingTime).Seconds() > 10 {
		//	_ = fmt.Sprintf("error, stack: %s", string(buf[:s]))
		//	// _ = DingDingSendText(dingUrl, errorInfo, nil)
		//	SetConfig("last_ding_time", time.Now())
		//}
	}
}

func ParamHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		req := ctx.Request
		if err := req.ParseMultipartForm(1024 * 1024); err != nil {
			if err != http.ErrNotMultipart {
				logrus.Errorf("error on parse multipart form array: %v", err)
			}
		}
	}
}

func ExtraHandler(data map[string]interface{}) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		for k, v := range data {
			ctx.Set(k, v)
		}
		ctx.Next()
	}
}

type HelloHandler struct {
	BaseHandler
}

func (h *HelloHandler) GET(ctx *gin.Context) {
	offline := GetConfigDefault("service_offline", false).(bool)
	if offline {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code": http.StatusInternalServerError,
		})
	} else {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusOK,
		})
	}
}

func (h *HelloHandler) POST(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"code": http.StatusOK,
	})
}
