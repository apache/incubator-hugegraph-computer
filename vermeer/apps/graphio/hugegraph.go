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

package graphio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/options"
	pd "vermeer/apps/protos/hugegraph-pd-grpc"
	"vermeer/apps/protos/hugegraph-pd-grpc/metapb"
	hstore "vermeer/apps/protos/hugegraph-store-grpc"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"google.golang.org/grpc/metadata"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	LoadMakers[LoadTypeHugegraph] = &HugegraphMaker{}
}

type HugegraphMaker struct{}

func (a *HugegraphMaker) CreateGraphLoader() GraphLoader {
	return &HugegraphLoader{}
}

func (a *HugegraphMaker) CreateGraphWriter() GraphWriter {
	return &HugegraphWriter{}
}

func (a *HugegraphMaker) MakeTasks(params map[string]string, taskID int32) ([]LoadPartition, error) {
	loadParts := make([]LoadPartition, 0)

	//连接pd，获取图的分区列表
	pdIPAddress := options.GetSliceString(params, "load.hg_pd_peers")
	graphName := options.GetString(params, "load.hugegraph_name")
	pdIPAddr, err := common.FindValidPD(pdIPAddress)
	if err != nil {
		return nil, err
	}

	//正式建立连接，进行查询
	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	pdConn, err := grpc.Dial(pdIPAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Errorf("connect hugegraph-pb err:%v", err)
		return nil, err
	}
	pdAuthority := common.PDAuthority{}
	ctx = pdAuthority.SetAuthority(ctx)
	var md metadata.MD
	pdClient := pd.NewPDClient(pdConn)
	partitionsResp, err := pdClient.QueryPartitions(ctx,
		&pd.QueryPartitionsRequest{Query: &metapb.PartitionQuery{GraphName: &graphName}}, grpc.Header(&md))
	if err != nil {
		logrus.Infof("QueryPartitions err:%v", err)
		return nil, err
	}
	pdAuthority.SetToken(md)

	// 获取用户设置的分区列表,如果为空，则获取所有分区
	userSetPartitions := options.GetSliceInt(params, "load.hg_partitions")
	userSetPartitionMap := make(map[uint32]struct{})
	for _, partitionID := range userSetPartitions {
		userSetPartitionMap[uint32(partitionID)] = struct{}{}
	}
	logrus.Debugf("user partition:%v", userSetPartitions)

	partitions := partitionsResp.GetPartitions()

	leaderStore := make(map[uint64]string)
	partID := int32(1)
	for _, partition := range partitions {
		// 用户设置了分区列表，则只加载用户设置的分区
		// 如果设置为空，则加载全部分区
		if len(userSetPartitionMap) != 0 {
			if _, ok := userSetPartitionMap[partition.Id]; !ok {
				continue
			}
		}
		var leaderStoreID uint64
		ctx = pdAuthority.SetAuthority(ctx)
		var md metadata.MD
		shardGroup, err := pdClient.GetShardGroup(ctx, &pd.GetShardGroupRequest{GroupId: partition.Id}, grpc.Header(&md))
		if err != nil {
			logrus.Errorf("GetShardGroup err:%v", err)
			return nil, err
		}
		pdAuthority.SetToken(md)
		shards := shardGroup.GetShardGroup().GetShards()
		for _, shard := range shards {
			//找到partition的leader store_id
			if shard.Role == metapb.ShardRole_Leader {
				leaderStoreID = shard.StoreId
				break
			}
		}
		//重复leader不再执行获取地址
		if _, ok := leaderStore[leaderStoreID]; !ok {
			//获得新的leader的地址并写入map
			ctx = pdAuthority.SetAuthority(ctx)
			var md metadata.MD
			storeResp, err := pdClient.GetStore(ctx, &pd.GetStoreRequest{StoreId: leaderStoreID}, grpc.Header(&md))
			if err != nil {
				logrus.Errorf("GetStore %v err:%v", leaderStoreID, err)
				return nil, err
			}
			pdAuthority.SetToken(md)
			if storeResp.Store.State != metapb.StoreState_Up {
				logrus.Errorf("store:%v state not up:%v", storeResp.GetStore().Address, storeResp.Store.State)
				logrus.Errorf("partition id:%v not available", partition.Id)
				return nil, fmt.Errorf("store:%v state not up:%v", storeResp.GetStore().Address, storeResp.Store.State)
			}
			leaderStore[leaderStoreID] = storeResp.GetStore().Address
		}
		//将一个partition细分为n个load任务（暂不支持）
		partitionCount := partition.EndKey - partition.StartKey
		var n uint64 = 1
		//parallel := uint64(options.GetInt(params, "load.parallel"))
		//if partitionCount > parallel {
		//	n = parallel
		//}
		partCount := partitionCount / n
		for i := uint64(0); i < n; i++ {
			vStart := partition.StartKey + i*partCount
			vEnd := vStart + partCount
			if vEnd > partition.EndKey || i == n-1 {
				vEnd = partition.EndKey
			}

			vertexPart := LoadPartition{}
			vertexPart.Init(partID, taskID, LoadPartTypeVertex)
			vertexPart.Params = make(map[string]string)
			vertexPart.Params["graph_name"] = graphName
			vertexPart.Params["part_type"] = LoadPartTypeVertex
			vertexPart.Params["partition_id"] = strconv.FormatUint(uint64(partition.Id), 10)
			vertexPart.Params["store_id"] = strconv.FormatUint(leaderStoreID, 10)
			vertexPart.Params["store_address"] = leaderStore[leaderStoreID]
			vertexPart.Params["start_key"] = strconv.FormatUint(vStart, 10)
			vertexPart.Params["end_key"] = strconv.FormatUint(vEnd, 10)
			loadParts = append(loadParts, vertexPart)
			partID += 1

			edgePart := LoadPartition{}
			edgePart.Init(partID, taskID, LoadPartTypeEdge)
			edgePart.Params = make(map[string]string)
			edgePart.Params["graph_name"] = graphName
			edgePart.Params["part_type"] = LoadPartTypeEdge
			edgePart.Params["partition_id"] = vertexPart.Params["partition_id"]
			edgePart.Params["store_id"] = vertexPart.Params["store_id"]
			edgePart.Params["store_address"] = leaderStore[leaderStoreID]
			edgePart.Params["start_key"] = vertexPart.Params["start_key"]
			edgePart.Params["end_key"] = vertexPart.Params["end_key"]
			loadParts = append(loadParts, edgePart)
			partID += 1
		}
	}
	for i := range loadParts {
		loadParts[i].Params["load.hugegraph_conditions"] = params["load.hugegraph_conditions"]
		loadParts[i].Params["load.vertex_property"] = params["load.vertex_property"]
		loadParts[i].Params["load.edge_property"] = params["load.edge_property"]
		loadParts[i].Params["load.hugegraph_vertex_condition"] = params["load.hugegraph_vertex_condition"]
		loadParts[i].Params["load.hugegraph_edge_condition"] = params["load.hugegraph_edge_condition"]
		loadParts[i].Params["load.hugestore_batch_timeout"] = params["load.hugestore_batch_timeout"]
		loadParts[i].Params["load.hugestore_batchsize"] = params["load.hugestore_batchsize"]
	}
	err = pdConn.Close()
	if err != nil {
		logrus.Errorf("hugegraph-pd close err:%v", err)
		return nil, err
	}
	return loadParts, nil
}

type HugegraphLoader struct {
	handler     *HStoreHandler
	schema      structure.PropertySchema
	count       int
	partitionID uint32
	startKey    uint32
	endKey      uint32
	graphName   string
	storeAddr   string
	useProperty bool
	useLabel    bool
}

func (hgl *HugegraphLoader) Init(params map[string]string, schema structure.PropertySchema) error {
	if options.GetInt(params, "load.use_property") == 1 {
		hgl.useProperty = true
	}
	hgl.useLabel = false
	hgl.schema = schema
	hgl.graphName = options.GetString(params, "graph_name")
	partitionId, err := strconv.ParseUint(options.GetString(params, "partition_id"), 10, 32)
	if err != nil {
		logrus.Errorf("get uint partition_id err:%v", err)
		return err
	}
	hgl.partitionID = uint32(partitionId)

	storeAddress := options.GetString(params, "store_address")
	hgl.storeAddr = storeAddress
	startKey, err := strconv.ParseUint(options.GetString(params, "start_key"), 10, 64)
	if err != nil {
		logrus.Errorf("get uint start_key err:%v", err)
		return err
	}
	hgl.startKey = uint32(startKey)

	endKey, err := strconv.ParseUint(options.GetString(params, "end_key"), 10, 64)
	if err != nil {
		logrus.Errorf("get uint end_key err:%v", err)
		return err
	}
	hgl.endKey = uint32(endKey)
	var condition string
	scanType := hstore.ScanPartitionRequest_SCAN_UNKNOWN
	loadPartType := options.GetString(params, "part_type")
	if loadPartType == LoadPartTypeVertex {
		scanType = hstore.ScanPartitionRequest_SCAN_VERTEX
		condition = options.GetString(params, "load.hugegraph_vertex_condition")
	} else if loadPartType == LoadPartTypeEdge {
		scanType = hstore.ScanPartitionRequest_SCAN_EDGE
		condition = options.GetString(params, "load.hugegraph_edge_condition")
	}

	propertyLabels := make([]int64, 0)
	if hgl.useProperty {
		var propLabels []string
		if loadPartType == LoadPartTypeVertex {
			propLabels = strings.Split(options.GetString(params, "load.vertex_property"), ",")
		} else if loadPartType == LoadPartTypeEdge {
			propLabels = strings.Split(options.GetString(params, "load.edge_property"), ",")
		}

		for _, label := range propLabels {
			if label == "" {
				continue
			}
			if strings.TrimSpace(label) == "label" {
				hgl.useLabel = true
				continue
			}
			iLabel, err := strconv.ParseInt(label, 10, 64)
			if err != nil {
				logrus.Errorf("property schema label type not int :%v", label)
				continue
			}
			propertyLabels = append(propertyLabels, iLabel)
		}
	} else {
		propertyLabels = []int64{-1}
	}
	if loadPartType == LoadPartTypeEdge {
		isVaild := false
		for _, prop := range propertyLabels {
			if prop >= 0 {
				isVaild = true
				break
			}
		}
		if !isVaild {
			hgl.useProperty = false
			logrus.Debugf("edge load without property")
		}
	}
	bacthSize := options.GetInt(params, "load.hugestore_batchsize")
	scanRequest := &hstore.ScanPartitionRequest_ScanRequest{
		ScanRequest: &hstore.ScanPartitionRequest_Request{
			ScanType:    scanType,
			GraphName:   hgl.graphName,
			PartitionId: hgl.partitionID,
			//StartCode:   hgl.startKey,
			//EndCode:     hgl.endKey,
			//Condition
			Condition: condition,
			Boundary:  0x10,
			//需要返回的property id,不填就返回全部。
			Properties: propertyLabels,
			BatchSize:  int32(bacthSize),
		},
	}

	storeTimeout := options.GetInt(params, "load.hugestore_batch_timeout")
	if storeTimeout < 60 {
		storeTimeout = 60
	}

	hgl.handler = &HStoreHandler{}
	err = hgl.handler.Init(scanRequest, storeAddress, int32(storeTimeout))
	if err != nil {
		logrus.Errorf("scan handler init error:%v", err)
		return err
	}

	return nil
}

func (hgl *HugegraphLoader) ReadVertex(vertex *structure.Vertex, property *structure.PropertyValue) error {
	hgVertex, err := hgl.handler.GetVertex()
	if err != nil {
		return err
	}
	vertex.ID, err = common.VariantToString(hgVertex.Id)
	if err != nil {
		return err
	}

	//read label
	vLabel := serialize.SString(hgl.schema.HgPSchema.Labels[int(hgVertex.GetLabel())])
	(*property)[0] = &vLabel
	//read property
	if hgl.useProperty {
		property.LoadFromHugegraph(hgVertex.GetProperties(), hgl.schema)
	}
	hgl.count += 1
	return nil
}

func (hgl *HugegraphLoader) ReadEdge(edge *structure.Edge, property *structure.PropertyValue) error {
	hgEdge, err := hgl.handler.GetEdge()
	if err != nil {
		return err
	}
	edge.Source, err = common.VariantToString(hgEdge.SourceId)
	if err != nil {
		return err
	}
	edge.Target, err = common.VariantToString(hgEdge.TargetId)
	if err != nil {
		return err
	}
	if hgl.useLabel {
		// read label
		eLabel := serialize.SString(hgl.schema.HgPSchema.Labels[int(hgEdge.GetLabel())])
		(*property)[0] = &eLabel
	}
	//read property
	if hgl.useProperty {
		property.LoadFromHugegraph(hgEdge.GetProperties(), hgl.schema)
	}
	hgl.count += 1
	return nil
}

func (hgl *HugegraphLoader) Name() string {
	return "hugegraph: " + hgl.graphName +
		", partition_id: " + strconv.FormatUint(uint64(hgl.partitionID), 10) +
		", store address: " + hgl.storeAddr +
		", start_key: " + strconv.FormatUint(uint64(hgl.startKey), 10) +
		", end_key: " + strconv.FormatUint(uint64(hgl.endKey), 10)
}

func (hgl *HugegraphLoader) ReadCount() int {
	return hgl.count
}

func (hgl *HugegraphLoader) Close() {
	hgl.handler.Close()
}

type HStoreHandler struct {
	client         hstore.GraphStore_ScanPartitionClient
	scanType       hstore.ScanPartitionRequest_ScanType
	cancel         context.CancelFunc
	storeConn      *grpc.ClientConn
	vertices       []*hstore.Vertex
	edges          []*hstore.Edge
	storeAddr      string
	offset         int
	noResponseTime int32
}

func (hs *HStoreHandler) Init(scanRequest *hstore.ScanPartitionRequest_ScanRequest, storeAddress string, storeTimeout int32) error {
	hs.offset = 0
	hs.scanType = scanRequest.ScanRequest.ScanType
	dialOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallSendMsgSize(4*1024*1024*1024),
		grpc.MaxCallRecvMsgSize(4*1024*1024*1024),
	)
	var err error
	hs.storeConn, err = grpc.Dial(storeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), dialOptions)
	if err != nil {
		logrus.Errorf("connect hugegraph-store %v error:%v", storeAddress, err)
		return err
	}
	storeClient := hstore.NewGraphStoreClient(hs.storeConn)
	ctx, cancel1 := context.WithCancel(context.Background())
	hs.cancel = cancel1
	hs.client, err = storeClient.ScanPartition(ctx)
	if err != nil {
		logrus.Errorf("get scan client error:%v", err)
		return err
	}
	err = hs.client.Send(&hstore.ScanPartitionRequest{Request: scanRequest})
	if err != nil {
		logrus.Errorf("hugegraph send scan request error:%v", err)
		return err
	}
	hs.storeAddr = storeAddress

	hs.noResponseTime = 0
	go func() {
		//监听时间，如果store超时无响应，cancel context
		for {
			time.Sleep(1 * time.Second)
			atomic.AddInt32(&hs.noResponseTime, 1)
			if atomic.LoadInt32(&hs.noResponseTime) >= storeTimeout {
				logrus.Errorf("store:%v partition_id:%v has no response for more than %ds. closing transport",
					storeAddress, scanRequest.ScanRequest.PartitionId, storeTimeout)
				hs.cancel()
				return
			} else if hs.noResponseTime < 0 {
				logrus.Infof("store:%v partition_id:%v is end. stopped listening", storeAddress, scanRequest.ScanRequest.PartitionId)
				return
			}
		}
	}()

	return nil
}

func (hs *HStoreHandler) Scan() error {
	resp, err := hs.client.Recv()
	if err != nil {
		if err != io.EOF {
			logrus.Infof("hugegraph store:%v recv error:%v", hs.storeAddr, err)
		}
		_ = hs.client.CloseSend()
		atomic.StoreInt32(&hs.noResponseTime, math.MinInt32)
		return err
	}

	atomic.StoreInt32(&hs.noResponseTime, 0)
	if hs.scanType == hstore.ScanPartitionRequest_SCAN_VERTEX {
		hs.vertices = resp.Vertex
		logrus.Debugf("get %d vertices", len(resp.Vertex))
	} else if hs.scanType == hstore.ScanPartitionRequest_SCAN_EDGE {
		hs.edges = resp.Edge
		logrus.Debugf("get %d edges", len(resp.Edge))
	}

	err = hs.client.Send(&hstore.ScanPartitionRequest{
		Request: &hstore.ScanPartitionRequest_ReplyRequest{
			ReplyRequest: &hstore.ScanPartitionRequest_Reply{SeqNo: 1}}})
	if err != nil && err != io.EOF {
		logrus.Infof("hugegraph send error:%v", err)
	}
	return nil
}

func (hs *HStoreHandler) GetVertex() (*hstore.Vertex, error) {
	if hs.offset == len(hs.vertices) {
		err := hs.Scan()
		if err != nil {
			return nil, err
		}
		hs.offset = 0
	}
	vertex := hs.vertices[hs.offset]
	hs.offset += 1
	return vertex, nil
}

func (hs *HStoreHandler) GetEdge() (*hstore.Edge, error) {
	if hs.offset == len(hs.edges) {
		err := hs.Scan()
		if err != nil {
			return nil, err
		}
		hs.offset = 0
	}
	edge := hs.edges[hs.offset]
	hs.offset += 1
	return edge, nil
}

func (hs *HStoreHandler) Close() {
	hs.cancel()
	err := hs.client.CloseSend()
	if err != nil {
		logrus.Infof("hugegraph client close error:%v", err)
	}
	err = hs.storeConn.Close()
	if err != nil {
		logrus.Infof("hugegraph conn close error:%v", err)
	}
}

type HugegraphWriter struct {
	count            int
	delimiter        string
	writerURL        string
	serverAddr       string
	hgSpace          string
	hGraph           string
	username         string
	password         string
	writeBackName    string
	vertexIDStrategy map[string]common.HgVertexIDType
	writerBody       []vertexComputeValue
	client           *http.Client
}

func (hgw *HugegraphWriter) Init(info WriterInitInfo) error {
	hgw.delimiter = options.GetString(info.Params, "output.delimiter")
	hgName := options.GetString(info.Params, "output.hugegraph_name")
	hgw.username = options.GetString(info.Params, "output.hugegraph_username")
	hgw.password = options.GetString(info.Params, "output.hugegraph_password")
	hgw.serverAddr = options.GetString(info.Params, "output.hugegraph_server")
	hgw.writeBackName = options.GetString(info.Params, "output.hugegraph_property")
	olapWriteType := options.GetString(info.Params, "output.hugegraph_write_type")
	if hgw.writeBackName == "" {
		hgw.writeBackName = options.GetString(info.Params, "compute.algorithm")
	}
	var err error
	hgw.hgSpace, hgw.hGraph, err = common.SplitHgName(hgName)
	if err != nil {
		return err
	}
	hgw.client = http.DefaultClient

	switch olapWriteType {
	case "OLAP_COMMON", "OLAP_SECONDARY", "OLAP_RANGE":

	default:
		return fmt.Errorf("output.hugegraph_write_type options only support OLAP_COMMON,OLAP_SECONDARY,OLAP_RANGE")
	}
	//创建一个propertyKey
	propertyKey := addPropertyKey{Name: hgw.writeBackName, WriteType: olapWriteType,
		DataType: info.OutputType, Cardinality: "SINGLE"}
	propKeyBytes, err := json.Marshal(propertyKey)
	if err != nil {
		return err
	}
	propURL := fmt.Sprintf("%s/graphspaces/%s/graphs/%s/schema/propertykeys", hgw.serverAddr, hgw.hgSpace, hgw.hGraph)
	propReq, err := http.NewRequest(http.MethodPost, propURL, bytes.NewReader(propKeyBytes))
	if err != nil {
		return err
	}
	propReq.SetBasicAuth(hgw.username, hgw.password)
	propReq.Header.Set("Content-Type", "application/json")
	resp, err := hgw.client.Do(propReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	logrus.Infof("set propertykeys status:%v", resp.Status)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		readAll, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("hugegraph propertykeys request failed:%v", string(readAll))
	}
	hgw.vertexIDStrategy = make(map[string]common.HgVertexIDType)
	//为hugegraph VertexLabel新增property
	for _, labelName := range info.HgVertexSchema.HgPSchema.Labels {
		vertexLabelURL := fmt.Sprintf("%s/graphspaces/%s/graphs/%s/schema/vertexlabels/%s?action=append",
			hgw.serverAddr, hgw.hgSpace, hgw.hGraph, labelName)
		vertexLabelBody := addVertexLabel{Name: labelName, Properties: []string{hgw.writeBackName},
			NullableKeys: []string{hgw.writeBackName}}
		vertexLabelBytes, err := json.Marshal(vertexLabelBody)
		if err != nil {
			return err
		}
		vertexLabelReq, err := http.NewRequest(http.MethodPut, vertexLabelURL, bytes.NewReader(vertexLabelBytes))
		if err != nil {
			return err
		}
		vertexLabelReq.Header.Set("Content-Type", "application/json")
		vertexLabelReq.SetBasicAuth(hgw.username, hgw.password)
		resp, err := hgw.client.Do(vertexLabelReq)
		if err != nil {
			return err
		}
		logrus.Infof("set vertex label status:%v", resp.Status)
		respBodyAll, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		respBody := make(map[string]any)
		err = json.Unmarshal(respBodyAll, &respBody)
		if err != nil {
			return err
		}
		idStrategy, ok := respBody["id_strategy"].(string)
		if !ok {
			return fmt.Errorf("get vertex label id_strategy not correct %v,%v", respBody["id_strategy"], respBody)
		}
		switch idStrategy {
		case "AUTOMATIC", "CUSTOMIZE_NUMBER":
			hgw.vertexIDStrategy[labelName] = common.HgVertexIDTypeNumber
		case "PRIMARY_KEY", "CUSTOMIZE_STRING":
			hgw.vertexIDStrategy[labelName] = common.HgVertexIDTypeString
		default:
			logrus.Errorf("vertex label id_strategy not supported %v", idStrategy)
		}
		_ = resp.Body.Close()
	}
	hgw.writerURL = fmt.Sprintf("%s/graphspaces/%s/graphs/%s/graph/vertices/batch",
		hgw.serverAddr, hgw.hgSpace, hgw.hGraph)

	//for _, schema := range info.HgVertexSchema.Schema {
	//	hgw.updateStrategies[schema.PropKey] = "OVERRIDE"
	//}
	hgw.initBody()
	return nil
}

func (hgw *HugegraphWriter) WriteVertex(vertexValue WriteVertexValue) {
	//写入post body
	var vertexID any

	if hgw.vertexIDStrategy[vertexValue.HgLabel] == common.HgVertexIDTypeString {
		vertexID = vertexValue.VertexID
	} else if hgw.vertexIDStrategy[vertexValue.HgLabel] == common.HgVertexIDTypeNumber {
		var err error
		vertexID, err = strconv.ParseInt(vertexValue.VertexID, 10, 64)
		if err != nil {
			logrus.Errorf("vertex id:%v hg type not number,err:%v", vertexID, err)
		}
	}
	vComputeValue := vertexComputeValue{
		ID:         vertexID,
		Properties: map[string]any{hgw.writeBackName: vertexValue.Value},
	}
	hgw.writerBody = append(hgw.writerBody, vComputeValue)
	//一次性发送不能超过2000个
	if len(hgw.writerBody) >= 1999 {
		err := hgw.sendReq()
		if err != nil {
			return
		}
		hgw.initBody()
	}
}

func (hgw *HugegraphWriter) WriteCount() int {
	return hgw.count
}

func (hgw *HugegraphWriter) initBody() {
	//初始化写入的request
	hgw.writerBody = make([]vertexComputeValue, 0)
}

func (hgw *HugegraphWriter) sendReq() error {
	//发送请求
	writerBodyBytes, err := json.Marshal(hgw.writerBody)
	if err != nil {
		logrus.Errorf("hugegraph writer json marshal err:%v", err)
		return err
	}
	writerRequest, err := http.NewRequest(http.MethodPost, hgw.writerURL, bytes.NewReader(writerBodyBytes))
	if err != nil {
		logrus.Errorf("new write request err:%v", err)
		return err
	}
	writerRequest.Header.Set("Content-Type", "application/json")
	writerRequest.SetBasicAuth(hgw.username, hgw.password)
	resp, err := hgw.client.Do(writerRequest)
	if err != nil {
		logrus.Errorf("write to hugegraph err:%v", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		logrus.Errorf("write to hugegraph status not OK :%v", resp.Status)
		all, _ := io.ReadAll(resp.Body)
		logrus.Infof("body:%v", string(all))
		return err
	}
	hgw.count += len(hgw.writerBody)
	return nil
}

func (hgw *HugegraphWriter) WriteStatistics(statistics map[string]any) error {
	_ = statistics
	//todo: 方案待定，得商定写回hugegraph的方式。
	return fmt.Errorf("not implemented")
}

func (hgw *HugegraphWriter) Close() {
	//发送最后一批
	_ = hgw.sendReq()
	hgw.client.CloseIdleConnections()
	logrus.Infof("write to hugegraph success count:%v", hgw.count)
}

type addPropertyKey struct {
	Name        string `json:"name"`
	DataType    string `json:"data_type"`
	WriteType   string `json:"write_type"`
	Cardinality string `json:"cardinality"`
}

type addVertexLabel struct {
	Name         string   `json:"name"`
	Properties   []string `json:"properties"`
	NullableKeys []string `json:"nullable_keys"`
	UserData     struct{} `json:"user_data"`
}

type vertexComputeValue struct {
	ID         any            `json:"id"`
	Properties map[string]any `json:"properties"`
}
