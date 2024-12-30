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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
	"vermeer/apps/options"

	"github.com/Unknwon/goconfig"
	"github.com/sirupsen/logrus"

	"vermeer/apps/common"
	"vermeer/apps/master"
	"vermeer/apps/worker"
)

func LoadConfig() map[string]string {
	env := flag.String("env", "default", "config environment")
	httpPeer := flag.String("http_peer", "", "http listen address")
	grpcPeer := flag.String("grpc_peer", "", "grpc listen address")
	masterPeer := flag.String("master_peer", "", "master grpc address")
	prefix := flag.String("log_prefix", "", "log file full path")
	runMode := flag.String("run_mode", "", "server run mode: master, worker")
	logLevel := flag.String("log_level", "", "log level")
	debugMode := flag.String("debug_mode", "", "debug mode")
	periods := flag.String("periods", "", "periods handler")
	funName := flag.String("func_name", "", "preprocess handler")
	taskParallelNum := flag.String("task_parallel_num", "", "task parallel num")
	auth := flag.String("auth", "", "authentication type: none, token")
	authTokenFactor := flag.String("auth_token_factor", "", "token generating factor, at least 4 characters")
	flag.Parse()

	config := map[string]string{
		"env":         *env,
		"http_peer":   "0.0.0.0:6688",
		"grpc_peer":   "0.0.0.0:6689",
		"master_peer": "0.0.0.0:6689",
		"log_prefix":  common.ServiceName + ".log",
		"run_mode":    "master",
	}

	configFile := "config/" + *env + ".ini"
	if strings.HasPrefix(*env, "/") {
		configFile = *env
	}
	cfg, err := goconfig.LoadConfigFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("load config error: %s", err))
	}
	config2, _ := cfg.GetSection("default")
	for k, v := range config2 {
		config[k] = v
	}

	config["config_path"] = configFile
	config["__env__"] = *env

	if *httpPeer != "" {
		config["http_peer"] = *httpPeer
	}
	if *grpcPeer != "" {
		config["grpc_peer"] = *grpcPeer
	}
	if *masterPeer != "" {
		config["master_peer"] = *masterPeer
	}
	if *prefix != "" {
		config["log_prefix"] = *prefix
	}
	if *runMode != "" {
		config["run_mode"] = *runMode
	}
	if *periods != "" {
		config["periods"] = *periods
	}
	if *funName != "" {
		config["func_name"] = *funName
	}
	if *taskParallelNum != "" {
		config["task_parallel_num"] = *taskParallelNum
	}
	if *auth != "" {
		config["auth"] = *auth
	}
	if *authTokenFactor != "" {
		config["auth_token_factor"] = *authTokenFactor
	}
	if *logLevel != "" {
		config["log_level"] = *logLevel
	}
	if *debugMode != "" {
		config["debug_mode"] = *debugMode
	}
	for _, section := range cfg.GetSectionList() {
		if section == "default" {
			continue
		}
		subCfg, _ := cfg.GetSection(section)
		b, _ := json.Marshal(subCfg)
		config[section] = string(b)
	}

	return config
}

func InitLogger(logLevelStr string) error {
	logLevel, err := logrus.ParseLevel(logLevelStr)
	if err != nil {
		fmt.Println("log ParseLevel error: ", err)
		return err
	}

	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logLevel)
	logrus.SetFormatter(&common.MyFormatter{})
	return nil
}

func main() {

	rand.Seed(time.Now().UnixNano())

	options.Init()
	config := LoadConfig()
	common.ConfigerInit(config)
	_ = InitLogger(common.GetConfigDefault("log_level", "info").(string))

	bg := new(int32)
	*bg = 0
	common.SetConfig("back_goroutines", bg)

	wg := new(sync.WaitGroup)
	common.SetConfig("wait_group", wg)

	runMode := common.GetConfig("run_mode").(string)

	if runMode == "master" {
		master.Main()
	} else if runMode == "worker" {
		worker.Main()
	}

	common.SetConfig("stop_flag", 1)
	logrus.Infof("Wait for goroutines done...")
	WaitTimeOut(wg, 10)

	logrus.Infof("Server exiting")
}

func WaitTimeOut(wg *sync.WaitGroup, seconds int) {
	var ch = make(chan bool)
	go func() {
		wg.Wait()
		ch <- false
	}()

	select {
	case <-time.After(time.Duration(seconds) * time.Second):
		logrus.Infof("wait time out...")
	case <-ch:
		logrus.Info("wait done...")
	}
}

//func RunPeriod(periods []string) {
//
//	// new post handler object
//	periodHandlers := make([]common.PeriodHandler, 0)
//	periodRouters := PeriodRouter()
//	for _, name := range periods {
//		if obj, ok := periodRouters[name]; ok {
//			err := obj.Init()
//			if err != nil {
//				logrus.Errorf("Init Period Handler Error: %v", err)
//				return
//			}
//			periodHandlers = append(periodHandlers, obj)
//		}
//	}
//
//	for {
//		if common.GetConfigDefault("stop_flag", 0).(int) == 1 {
//			break
//		}
//
//		wg := common.GetConfig("wait_group").(*sync.WaitGroup)
//		wg.Add(1)
//		logrus.Infof("run period start...")
//
//		for _, obj := range periodHandlers {
//			obj.Process()
//		}
//
//		time.Sleep(1 * time.Second)
//		logrus.Infof("run period end")
//		wg.Done()
//	}
//}
