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
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var PrometheusMetrics MozartPrometheus

type MozartPrometheus struct {
	serviceName    string
	reqCnt         *prometheus.CounterVec
	reqDur         *prometheus.SummaryVec
	slowReqCnt     *prometheus.CounterVec
	TaskCnt        *prometheus.CounterVec
	TaskRunningCnt *prometheus.GaugeVec
	GraphCnt       *prometheus.GaugeVec
	GraphLoadedCnt *prometheus.GaugeVec
	WorkerCnt      *prometheus.GaugeVec
	VertexCnt      *prometheus.GaugeVec
	EdgeCnt        *prometheus.GaugeVec
}

func (p *MozartPrometheus) HandleFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		status := strconv.Itoa(c.Writer.Status())
		elapsed := time.Since(start) / time.Millisecond
		reqUri := c.Request.URL.Path
		action := c.DefaultQuery("action", "")
		reqUri += "/" + action
		p.reqCnt.WithLabelValues(status, c.Request.Method, c.Request.Host, reqUri).Inc()
		p.reqDur.WithLabelValues(status, c.Request.Method, c.Request.Host, reqUri).Observe(float64(elapsed))
		if elapsed > 20 {
			p.slowReqCnt.WithLabelValues(c.Request.Host, reqUri, "20").Inc()
		}
		if elapsed > 200 {
			p.slowReqCnt.WithLabelValues(c.Request.Host, reqUri, "200").Inc()
		}
	}
}

func (p *MozartPrometheus) register() {
	_ = prometheus.Register(p.reqCnt)
	_ = prometheus.Register(p.reqDur)
	_ = prometheus.Register(p.slowReqCnt)
	_ = prometheus.Register(p.TaskCnt)
	_ = prometheus.Register(p.TaskRunningCnt)
	_ = prometheus.Register(p.GraphCnt)
	_ = prometheus.Register(p.GraphLoadedCnt)
	_ = prometheus.Register(p.WorkerCnt)
	_ = prometheus.Register(p.VertexCnt)
	_ = prometheus.Register(p.EdgeCnt)
}

func InitMetrics(engine *gin.Engine, serviceName string) {
	reqCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: serviceName,
			Name:      "requests_total",
			Help:      "How many HTTP requests processed, partitioned by status code and HTTP method.",
		},
		[]string{"code", "method", "host", "url"})
	reqDur := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Subsystem:  serviceName,
			Name:       "request_duration_ms",
			Help:       "The HTTP request latencies in ms.",
			Objectives: map[float64]float64{0.9: 0, 0.95: 0, 0.99: 0},
			MaxAge:     10 * time.Second,
			AgeBuckets: 3,
		},
		[]string{"code", "method", "host", "url"})
	slowReqCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: serviceName,
			Name:      "requests_slow",
			Help:      "How many HTTP requests longer than xx ms.",
		},
		[]string{"host", "url", "level"})
	taskCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: serviceName,
			Name:      "task_count_total",
			Help:      "How many tasks created, partitioned by task type.",
		},
		[]string{"task_type"})
	taskRunningCnt := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: serviceName,
			Name:      "task_running_total",
			Help:      "How many tasks is running, partitioned by task type.",
		},
		[]string{"task_type"})
	graphCnt := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: serviceName,
			Name:      "graph_total",
			Help:      "How many graphs is created",
		},
		[]string{})
	graphLoadedCnt := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: serviceName,
			Name:      "graph_loaded_total",
			Help:      "How many graphs is loaded",
		},
		[]string{})
	workerCnt := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: serviceName,
			Name:      "worker_count",
			Help:      "How many workers is available",
		},
		[]string{})
	vertexCnt := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: serviceName,
			Name:      "vertex_count",
			Help:      "How many vertices in graphs, partitioned by graph name.",
		},
		[]string{"graph_name"})
	edgeCnt := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: serviceName,
			Name:      "edge_count",
			Help:      "How many edges in graphs, partitioned by graph name.",
		},
		[]string{"graph_name"})
	PrometheusMetrics = MozartPrometheus{
		serviceName:    serviceName,
		reqCnt:         reqCnt,
		reqDur:         reqDur,
		slowReqCnt:     slowReqCnt,
		TaskCnt:        taskCnt,
		TaskRunningCnt: taskRunningCnt,
		GraphCnt:       graphCnt,
		GraphLoadedCnt: graphLoadedCnt,
		WorkerCnt:      workerCnt,
		VertexCnt:      vertexCnt,
		EdgeCnt:        edgeCnt,
	}
	PrometheusMetrics.register()
	engine.Use(PrometheusMetrics.HandleFunc())
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))
}
