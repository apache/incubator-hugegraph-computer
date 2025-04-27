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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type Sentinel struct {
	engine *gin.Engine
	srv    *http.Server
}

func (sen *Sentinel) Init(peer string, debugMode string) {
	if debugMode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	srv := &http.Server{
		Addr:    peer,
		Handler: engine,
	}
	pprof.Register(engine, "/debug/pprof")
	engine.Use(LoggerHandler())
	engine.Use(ErrorHandler())
	engine.Use(gin.Recovery())
	engine.Use(ParamHandler())

	InitMetrics(engine, fmt.Sprintf("%s_%s", ServiceName, GetConfigDefault("run_mode", "").(string)))
	sen.engine = engine
	sen.srv = srv
}

func (sen *Sentinel) StaticFS(relativePath string, fs http.FileSystem) {
	sen.engine.StaticFS(relativePath, fs)
}
func (sen *Sentinel) StaticFolder(relativePath string, folder string) {
	sen.engine.Static(relativePath, folder)
}
func (sen *Sentinel) Run() {
	if err := sen.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logrus.Fatalf("listen error: %s", err)
		panic(err)
	}
}

func (sen *Sentinel) Shutdown() context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := sen.srv.Shutdown(ctx); err != nil {
		logrus.Fatalf("Server Shutdown: %v", err)
	}
	return cancel
}

func (sen *Sentinel) Register(routers map[string]BaseHandler) {
	for url, handler := range routers {
		sen.engine.GET(url, handler.GET)
		sen.engine.POST(url, handler.POST)
		sen.engine.PUT(url, handler.PUT)
		sen.engine.DELETE(url, handler.DELETE)
	}
}

func (sen *Sentinel) RegisterGroup(base string, routers map[string]BaseHandler, filters ...gin.HandlerFunc) {
	rg := sen.engine.Group(base, filters...)
	for url, handler := range routers {
		rg.GET(url, handler.GET)
		rg.POST(url, handler.POST)
		rg.PUT(url, handler.PUT)
		rg.DELETE(url, handler.DELETE)
	}
}

func (sen *Sentinel) RegisterAPI(verion int, base string, routers map[string]BaseHandler, filters ...gin.HandlerFunc) {
	root := fmt.Sprintf("/api/v%d%s", verion, base)
	rg := sen.engine.Group(root, filters...)

	for url, handler := range routers {
		rg.GET(url, handler.GET)
		rg.POST(url, handler.POST)
		rg.PUT(url, handler.PUT)
		rg.DELETE(url, handler.DELETE)
	}

}
