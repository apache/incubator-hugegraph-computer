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

package bl

import (
	"fmt"
	. "vermeer/apps/master/graphs"
	"vermeer/apps/structure"
)

// AdminBl Admin only
type AdminBl struct {
	Cred *structure.Credential
}

// SaveGraph
func (ab *AdminBl) SaveGraph(spaceName, graphName string) ([]*structure.GraphPersistenceInfo, error) {
	graph := graphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		return nil, fmt.Errorf("graph %v/%v not exist", spaceName, graphName)
	}

	if graph.State == structure.GraphStateOnDisk {
		return nil, fmt.Errorf("graph %v/%v has been saved", spaceName, graphName)
	}

	res, success := GraphPersistenceTask.Operate(spaceName, graphName, Save)
	if !success {
		return nil, fmt.Errorf("graph %v/%v do not save success", spaceName, graphName)
	}

	err := graphMgr.SaveInfo(graph.SpaceName, graph.Name)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ReadGraph
func (ab *AdminBl) ReadGraph(spaceName, graphName string) ([]*structure.GraphPersistenceInfo, error) {
	graph := graphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		return nil, fmt.Errorf("graph %v not exist", graphName)
	}

	if graph.State != structure.GraphStateOnDisk {
		return nil, fmt.Errorf("graph %v has not been saved", graphName)
	}

	res, success := GraphPersistenceTask.Operate(spaceName, graphName, Read)
	if !success {
		return nil, fmt.Errorf("graph %v do not read success", graphName)
	}
	accessMgr.Access(spaceName, graphName)

	return res, nil
}

func (ab *AdminBl) IsDispatchPaused() bool {
	return Scheduler.IsDispatchPaused()
}

func (ab *AdminBl) PauseDispatchTask() error {
	if err := serviceStore.SaveDispatchPause(true); err != nil {
		return err
	}
	Scheduler.PauseDispatch()
	return nil
}

func (ab *AdminBl) ResumeDispatchTask() error {
	if err := serviceStore.SaveDispatchPause(false); err != nil {
		return err
	}
	Scheduler.ResumeDispatch()
	return nil
}
