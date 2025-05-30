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

package workers

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"path"
	"reflect"
	"strings"
	"time"
	"vermeer/apps/common"
	pb "vermeer/apps/protos"
	storage "vermeer/apps/storage"
	"vermeer/apps/structure"

	"github.com/bwmarrin/snowflake"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	prefixMapper = "WORKER_MAPPER"
)

// WorkerClient
type WorkerClient struct {
	Id         int32     `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	GrpcPeer   string    `json:"grpc_peer,omitempty"`
	IpAddr     string    `json:"ip_addr,omitempty"`
	State      string    `json:"state,omitempty"`
	Version    string    `json:"version,omitempty"`
	Group      string    `json:"group,omitempty"`
	InitTime   time.Time `json:"init_time,omitempty"`
	LaunchTime time.Time `json:"launch_time,omitempty"`
	//LoadStream      pb.Master_LoadGraphTaskServer `json:"-"`
	//ComputeStream   pb.Master_ComputeTaskServer   `json:"-"`
	//SuperStepStream pb.Master_SuperStepServer     `json:"-"`
	Connection *grpc.ClientConn `json:"-"`
	Session    pb.WorkerClient  `json:"-"`
}

func (wc *WorkerClient) Close() {
	_ = wc.Connection.Close()
}

// workerManager Singleton
type workerManager struct {
	structure.MutexLocker
	workersByName map[string]*WorkerClient
	groupMapper   map[string]string //[space graph]:[group]
	idSeed        int32
	nameGenerator *snowflake.Node
	store         storage.Store
}

func (wm *workerManager) Init() {
	wm.workersByName = make(map[string]*WorkerClient)
	wm.groupMapper = make(map[string]string)
	wm.idSeed = 1

	var err error
	wm.nameGenerator, err = snowflake.NewNode(rand.Int63n(1023))
	if err != nil {
		logrus.Errorf("new snowflake error: %s", err)
	}

	p, err := common.GetCurrentPath()
	if err != nil {
		logrus.Errorf("get current path error:%v", err)
	}

	dir := path.Join(p, "vermeer_data", "worker_info")
	wm.store, err = storage.StoreMaker(storage.StoreOption{
		StoreName: storage.StoreTypePebble,
		Path:      dir,
		Fsync:     true,
	})
	if err != nil {
		panic(fmt.Errorf("failed to initialize the kv store, caused by: %v. maybe another vermeer master is running.", err))
	}

	wm.initMapper()
}

// CreateWorker Build a WorkerClient without an ID, and it'll receive one upon joining the WorkerManager.
// The new WokerClient instance will be assigned a same name with the old one added to the WorkerManager,
// which has the same workerPeer property.
func (wm *workerManager) CreateWorker(workerPeer string, ipAddr string, version string, workerGroup string) (*WorkerClient, error) {
	if workerPeer == "" {
		return nil, fmt.Errorf("the argument 'workerPeer' is invalid")
	}
	if ipAddr == "" {
		return nil, fmt.Errorf("the argument 'ipAddr' is invalid")
	}
	if version == "" {
		return nil, fmt.Errorf("the argument 'version' is invalid")
	}

	// must check if workerGroup is valid
	if workerGroup == "" {
		workerGroup = "$"
	}

	worker := &WorkerClient{
		GrpcPeer:   workerPeer,
		IpAddr:     ipAddr,
		Version:    version,
		LaunchTime: time.Now(),
		Group:      workerGroup,
	}

	workerInDB := wm.retrieveWorker(workerPeer)
	if workerInDB != nil {
		worker.Name = workerInDB.Name
		worker.InitTime = workerInDB.InitTime
		worker.Group = workerInDB.Group
	} else {
		worker.Name = wm.nameGenerator.Generate().String()
		worker.InitTime = worker.LaunchTime
	}

	// if workerGroup in workerInDB is different from the one in worker, give a warning to the user
	if workerGroup != "$" && worker.Group != workerGroup {
		logrus.Warnf("worker manager, worker group mismatch: given %s, but found %s in db for worker %s", workerGroup, worker.Group, worker.Name)
	}

	return worker, nil
}

// AddWorker
func (wm *workerManager) AddWorker(worker *WorkerClient) (int32, error) {
	if worker == nil {
		return -1, fmt.Errorf("the argument 'worker' is nil")
	}

	defer wm.Unlock(wm.Lock())

	if _, ok := wm.workersByName[worker.Name]; ok {
		return -1, fmt.Errorf("worker manager, worker name exists: %s", worker.Name)
	}

	worker.Id = wm.idSeed

	if worker.Group == "" {
		worker.Group = "$"
	}

	data := make(map[string]any, 0)
	process := make([]assure, 0)

	wm.putWorkerData(data, worker)
	process = append(process, func() { wm.workersByName[worker.Name] = worker })

	if err := wm.batchSave(data, process); err != nil {
		return -1, err
	}

	wm.idSeed++

	logrus.Infof("worker manager, add worker: %s, %d, %s", worker.Name, worker.Id, worker.GrpcPeer)
	common.PrometheusMetrics.WorkerCnt.WithLabelValues().Inc()

	return worker.Id, nil
}

func (wm *workerManager) RemoveWorker(name string) {
	defer wm.Unlock(wm.Lock())

	if _, ok := wm.workersByName[name]; !ok {
		logrus.Errorf("RemoveWorker worker manager, worker not exists: %s", name)
		return
	}

	delete(wm.workersByName, name)
	common.PrometheusMetrics.WorkerCnt.WithLabelValues().Dec()
	logrus.Infof("removed worker:%v ", name)
}

func (wm *workerManager) GetWorker(name string) *WorkerClient {
	defer wm.Unlock(wm.Lock())
	return wm.workersByName[name]
}
func (wm *workerManager) GetWorkerInfo(name string) *WorkerClient {
	defer wm.Unlock(wm.Lock())
	worker := wm.workersByName[name]

	if worker != nil {
		return worker
	}

	worker = wm.retrieveWorker(name)

	if worker != nil {
		worker.State = "OFFLINE"
		return worker
	}

	return nil
}

func (wm *workerManager) GetAllWorkers() []*WorkerClient {
	defer wm.Unlock(wm.Lock())
	return SortWorkersAsc(map2Workers(wm.workersByName))
}

func (wm *workerManager) CheckWorkerAlive(name string) bool {
	defer wm.Unlock(wm.Lock())
	worker, ok := wm.workersByName[name]

	if !ok || worker.Connection == nil || worker.Connection.GetState() != connectivity.Ready {
		return false
	}

	return true
}

func (wm *workerManager) SetWorkerGroup(workerName, workerGroup string) error {
	if workerName == "" {
		return fmt.Errorf("the argument 'workerName' is invalid")
	}

	if workerGroup == "" {
		workerGroup = "$"
	}

	defer wm.Unlock(wm.Lock())

	workerInfo := wm.retrieveWorker(workerName)
	if workerInfo == nil {
		return fmt.Errorf("SetWorkerGroup worker manager, worker not exists: %s", workerName)
	}

	workerInfo.Group = workerGroup

	data := make(map[string]any, 0)
	process := make([]assure, 0)

	wm.putWorkerData(data, workerInfo)
	process = append(process, func() {
		if worker, ok := wm.workersByName[workerName]; ok {
			worker.Group = workerGroup
		}
	})

	if err := wm.batchSave(data, process); err != nil {
		return err
	}

	logrus.Infof("worker manager, set worker group: %s, %s", workerName, workerGroup)

	return nil
}

// AllocWorkerSpace Exclusively allocate a WorkerGroup to a specific space, and subsequently
// remove its associations from other spaces and graphs.
func (wm *workerManager) AllocGroupSpace(workerGroup, spaceName string) error {
	if workerGroup == "" {
		return fmt.Errorf("the 'workerGroup' argument is invalid")
	}
	if spaceName == "" {
		return fmt.Errorf("the 'spaceName' argument is invalid")
	}

	defer wm.Unlock(wm.Lock())

	data := make(map[string]any, 0)
	wm.putClearMapperData(data, workerGroup)
	wm.putMapperData(data, workerGroup, spaceName)

	process := []assure{
		func() { wm.clearMapping(workerGroup) },
		func() { wm.allocGroup(workerGroup, spaceName) },
	}

	if err := wm.batchSave(data, process); err != nil {
		return err
	}

	return nil
}

// AllocateGraph AllocateSpace Exclusively allocate a WorkerClient to a specific graph, and subsequently
func (wm *workerManager) UnallocGroup(workerGroup string) (int, error) {
	if workerGroup == "" {
		return 0, fmt.Errorf("the 'workerGroup' argument is invalid")
	}

	defer wm.Unlock(wm.Lock())

	data := make(map[string]any, 0)
	num := wm.putClearMapperData(data, workerGroup)

	process := []assure{
		func() { wm.clearMapping(workerGroup) },
	}

	if err := wm.batchSave(data, process); err != nil {
		return 0, err
	}

	return num, nil
}

// AllocateSpace Exclusively allocate a WorkerGroup to a specific graph, and subsequently
// remove its associations from other spaces and graphs.
func (wm *workerManager) AllocGroupGraph(workerGroup, spaceName, graphName string) error {
	if workerGroup == "" {
		return fmt.Errorf("the 'workerGroup' argument is invalid")
	}
	if spaceName == "" {
		return fmt.Errorf("the 'spaceName' argument is invalid")
	}
	if graphName == "" {
		return fmt.Errorf("the 'graphName' argument is invalid")
	}

	defer wm.Unlock(wm.Lock())

	data := make(map[string]any, 0)
	wm.putClearMapperData(data, workerGroup)
	wm.putMapperData(data, workerGroup, spaceName, graphName)

	process := []assure{
		func() { wm.clearMapping(workerGroup) },
		func() { wm.allocGroup(workerGroup, spaceName, graphName) },
	}

	if err := wm.batchSave(data, process); err != nil {
		return err
	}

	return nil
}

// AllocateSpace Allocate a WorkerGroup to a space without removing its association with other spaces or graphs.
func (wm *workerManager) ShareSpace(workerGroup, spaceName string) error {
	if workerGroup == "" {
		return fmt.Errorf("the 'workerGroup' argument is invalid")
	}
	if spaceName == "" {
		return fmt.Errorf("the 'spaceName' argument is invalid")
	}

	defer wm.Unlock(wm.Lock())

	data := make(map[string]any, 0)
	wm.putMapperData(data, workerGroup, spaceName)
	process := []assure{func() { wm.allocGroup(workerGroup, spaceName) }}

	if err := wm.batchSave(data, process); err != nil {
		return err
	}

	return nil
}

// AllocateGraph Allocate a WorkerGroup to a Graph wihout removing its association with other spaces or graphs.
func (wm *workerManager) ShareGraph(workerGroup, spaceName, graphName string) error {
	if workerGroup == "" {
		return fmt.Errorf("the 'workerGroup' argument is invalid")
	}
	if spaceName == "" {
		return fmt.Errorf("the 'spaceName' argument is invalid")
	}
	if graphName == "" {
		return fmt.Errorf("the 'graphName' argument is invalid")
	}

	defer wm.Unlock(wm.Lock())

	data := make(map[string]any, 0)
	wm.putMapperData(data, workerGroup, spaceName, graphName)
	processes := []assure{func() { wm.allocGroup(workerGroup, spaceName, graphName) }}

	if err := wm.batchSave(data, processes); err != nil {
		return err
	}

	return nil
}

func (wm *workerManager) AllSpaceWorkers() map[string][]*WorkerClient {
	defer wm.Unlock(wm.Lock())
	res := make(map[string][]*WorkerClient, len(wm.groupMapper))

	for owner, group := range wm.groupMapper {
		if owner == "" {
			continue
		}
		// owner: "spaceName graphName"
		if strings.Contains(owner, " ") {
			continue
		}
		if group == "" || group == "$" {
			continue
		}

		workers := wm.getGroupWorkers(group)

		if len(workers) > 0 {
			res[owner+"@"+group] = SortWorkersAsc(workers)
		} else {
			res[owner+"@"+group] = make([]*WorkerClient, 0)
		}
	}

	return res
}

func (wm *workerManager) AllGraphWorkers() map[string][]*WorkerClient {
	defer wm.Unlock(wm.Lock())
	res := make(map[string][]*WorkerClient, len(wm.groupMapper))
	for owner, group := range wm.groupMapper {
		// owner: "spaceName graphName"
		if !strings.Contains(owner, " ") {
			continue
		}

		owner = strings.ReplaceAll(owner, " ", "/")

		workers := wm.getGroupWorkers(group)

		if len(workers) > 0 {
			res[owner] = SortWorkersAsc(workers)
		}

	}

	return res
}

func (wm *workerManager) AllGroupWorkers() map[string][]*WorkerClient {
	defer wm.Unlock(wm.Lock())
	res := make(map[string][]*WorkerClient)

	for _, worker := range wm.workersByName {
		if worker.Group == "" || worker.Group == "$" {
			continue
		}

		workers := res[worker.Group]

		if workers == nil {
			workers = make([]*WorkerClient, 0)
		}

		res[worker.Group] = append(workers, worker)
	}

	for key, workers := range res {
		res[key] = SortWorkersAsc(workers)
	}

	return res
}

func (wm *workerManager) CommonWorkers() map[string][]*WorkerClient {
	defer wm.Unlock(wm.Lock())
	res := make(map[string][]*WorkerClient, 2)
	res["no_group"] = SortWorkersAsc(wm.noGroupWorkers())
	return res
}

// ApplyWorkers GetWorkers Find and return the collection of WorkerClient instance that match the arguments: spaceName and graphName.
// It will collect the item assigned to the graph firstly.
// If no items are found, it will then try to find the items with space name.
// In the end, if no items are found, the items belonging to the common category will be returned.
func (wm *workerManager) ApplyWorkers(spaceName string, graphName string) (workers []*WorkerClient) {
	defer wm.Unlock(wm.Lock())

	var buf []*WorkerClient

	if group, ok := wm.groupMapper[wm.toOwnerKey(spaceName, graphName)]; ok {
		buf = wm.getGroupWorkers(group)
		if len(buf) > 0 {
			return SortWorkersAsc(buf)
		}
	}

	if group, ok := wm.groupMapper[spaceName]; ok {
		buf = wm.getGroupWorkers(group)
		if len(buf) > 0 {
			return SortWorkersAsc(buf)
		}
	}

	return SortWorkersAsc(wm.commonWorkers())
}

func (wm *workerManager) ApplyGroup(spaceName string, graphName string) (groupName string) {
	defer wm.Unlock(wm.Lock())

	if group, ok := wm.groupMapper[wm.toOwnerKey(spaceName, graphName)]; ok {
		return group
	}

	if group, ok := wm.groupMapper[spaceName]; ok {
		return group
	}

	return "$"
}

func (wm *workerManager) GroupWorkers(workerGroup string) []*WorkerClient {
	defer wm.Unlock(wm.Lock())
	return wm.getGroupWorkers(workerGroup)
}

func (wm *workerManager) GroupWorkerMap(workerGroup string) map[string]*WorkerClient {
	defer wm.Unlock(wm.Lock())
	return wm.getGroupWorkerMap(workerGroup)
}

func (wm *workerManager) initMapper() {
	mapperKeys := make([]string, 0)

	for kv := range wm.store.Scan() {
		if strings.HasPrefix(string(kv.Key), prefixMapper) {
			mapperKeys = append(mapperKeys, string(kv.Key))
			continue
		}
	}

	for _, s := range mapperKeys {
		if buf, err := wm.store.Get([]byte(s)); err == nil {
			v := string(buf)

			if v == "" {
				logrus.Warnf("aborted to restore a worker group mapping caused by empty group, key: %s", s)
				continue
			}

			keys := strings.Split(s, " ")
			keylen := len(keys)

			if keylen < 2 {
				logrus.Errorf("aborted to restore a worker group mapping caused by illegal length of keys, length: %d, keys: %s", keylen, keys)
				continue
			}

			wm.allocGroup(v, keys[1:]...)
			logrus.Infof("restored a worker group mapping, keys: %s, group: %s", keys, v)
		}

	}

}

func (wm *workerManager) getGroupWorkers(workerGroup string) []*WorkerClient {
	workers := make([]*WorkerClient, 0)

	for _, w := range wm.workersByName {
		if w.Group == workerGroup {
			workers = append(workers, w)
		}
	}

	return workers
}

func (wm *workerManager) getGroupWorkerMap(workerGroup string) map[string]*WorkerClient {
	workerMap := make(map[string]*WorkerClient)

	for _, w := range wm.workersByName {
		if w.Group == workerGroup {
			workerMap[w.Name] = w
		}
	}

	return workerMap
}

func (wm *workerManager) putWorkerData(data map[string]any, worker *WorkerClient) {
	data[worker.GrpcPeer] = worker
	data[worker.Name] = worker
}

func (wm *workerManager) putMapperData(data map[string]any, workerGroup string, owner ...string) {
	key := []string{prefixMapper}
	key = append(key, owner...)
	data[strings.Join(key, " ")] = workerGroup
}

func (wm *workerManager) putClearMapperData(data map[string]any, workerGroup string) int {
	num := 0
	for k, v := range wm.groupMapper {
		if v == workerGroup {
			key := []string{prefixMapper, k}
			data[strings.Join(key, " ")] = ""
			num++
		}
	}
	return num
}

// Retrieve a collection of workers that are shareable by any space or graph,
func (wm *workerManager) commonWorkers() []*WorkerClient {
	return wm.noGroupWorkers()
}

// Locate and retrieve a collection of workers without any worker groups
func (wm *workerManager) noGroupWorkers() []*WorkerClient {
	workers := make([]*WorkerClient, 0, len(wm.workersByName))

	for _, worker := range wm.workersByName {
		if worker.Group == "" || worker.Group == "$" {
			workers = append(workers, worker)
		}
	}

	return workers
}

// retrieveWorker Retrieve a worker from obj-store.
func (wm *workerManager) retrieveWorker(workerKey string) *WorkerClient {
	workerInDB := &WorkerClient{}

	if err := wm.retrieveObject(workerKey, workerInDB); err != nil {
		logrus.Infof("failed to retrieve a woker with key:%s from store, caused by: %v", workerKey, err)
		return nil
	}

	return workerInDB
}

// saveWorker Save worker info to the obj-store.
func (wm *workerManager) saveWorker(worker *WorkerClient) error {
	bytes, err := json.Marshal(worker)
	if err != nil {
		return fmt.Errorf("json marshal worker info error:%w", err)
	}

	err = wm.store.Set([]byte(worker.GrpcPeer), bytes)
	if err != nil {
		return fmt.Errorf("store set worker info error:%w", err)
	}

	return nil
}

func (wm *workerManager) batchSave(data map[string]any, processes []assure) error {
	if err := atomProcess(func() error { return wm.saveObjects(data) }, processes); err != nil {
		return err
	}

	return nil
}

func (wm *workerManager) saveObjects(kv map[string]any) error {
	batch := wm.store.NewBatch()

	for k, v := range kv {
		var bytes []byte
		var err error

		if str, ok := v.(string); ok {
			bytes = []byte(str)
		} else {
			bytes, err = json.Marshal(v)
		}

		if err != nil {
			return fmt.Errorf("json marshal '%s' error: %w", reflect.TypeOf(v), err)
		}

		if err = batch.Set([]byte(k), bytes); err != nil {
			return err
		}
	}

	err := batch.Commit()

	if err != nil {
		return fmt.Errorf("store batch set error: %w", err)
	}

	return nil
}

func (wm *workerManager) retrieveObject(key string, obj any) error {
	value, err := wm.store.Get([]byte(key))
	if err != nil {
		return err
	}

	err = json.Unmarshal(value, obj)
	if err != nil {
		return err
	}

	return nil
}

func (wm *workerManager) clearMapping(workerGroup string) {
	for k, g := range wm.groupMapper {
		if g == workerGroup {
			delete(wm.groupMapper, k)
		}
	}
}

func (wm *workerManager) GetWorkerByName(workerName string) (*WorkerClient, error) {
	worker := wm.workersByName[workerName]

	if worker == nil {
		return nil, fmt.Errorf("no worker with name '%s' exists", workerName)
	}

	return worker, nil
}

func (wm *workerManager) allocGroup(workerGroup string, owner ...string) {
	key := wm.toOwnerKey(owner...)
	wm.groupMapper[key] = workerGroup
}

func (wm *workerManager) toOwnerKey(owner ...string) string {
	return strings.Join(owner, " ")
}

// // GetSpaceWorkers Return the collection of WorkerClient instances belonging the space.
// func (wm *WorkerManager) GetSpaceWorkers(spaceName string) []*WorkerClient {
// 	defer wm.Unlock(wm.Lock())
// 	return sortWorkersAsc(wm.mapper2Workers(wm.groupMapper[spaceName]))
// }

// func (wm *WorkerManager) mapper2Workers(mapper map[string]string) []*WorkerClient {
// 	return wm.doMapper2Workers(mapper, func(name, peer string) *WorkerClient {
// 		return wm.workersByName[name]
// 	})
// }

// func (wm *WorkerManager) mapper2WorkerInfo(mapper map[string]string) []*WorkerClient {
// 	return wm.doMapper2Workers(mapper, func(name, peer string) *WorkerClient {
// 		if worker := wm.workersByName[name]; worker != nil {
// 			return worker
// 		}
// 		return wm.retrieveWorker(peer)
// 	})
// }

// func (wm *WorkerManager) doMapper2Workers(mapper map[string]string, apply func(name, peer string) *WorkerClient) []*WorkerClient {
// 	if mapper == nil {
// 		return make([]*WorkerClient, 0)
// 	}

// 	workers := make([]*WorkerClient, 0, len(mapper))
// 	for name, peer := range mapper {
// 		worker := apply(name, peer)
// 		if worker == nil {
// 			logrus.Warnf("worker with name '%s' or peer '%s' does not exists", name, peer)
// 			continue
// 		}
// 		if worker.Connection == nil {
// 			worker.State = "OFFLINE"
// 		} else {
// 			worker.State = worker.Connection.GetState().String()
// 		}
// 		workers = append(workers, worker)
// 	}

// 	return workers
// }
