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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pd "vermeer/apps/protos/hugegraph-pd-grpc"
	hstore "vermeer/apps/protos/hugegraph-store-grpc"
)

const (
	HgVertexIDTypeString HgVertexIDType = iota
	HgVertexIDTypeNumber
)

const (
	HgValueTypeInt    = "INT"
	HgValueTypeFloat  = "FLOAT"
	HgValueTypeString = "TEXT"
)

type HgVertexIDType uint16

func FindValidPD(pdIPAddress []string) (string, error) {
	// retry 3 times to find valid pd address
	var pdIPAddr string
	var err error
	for i := 0; i < 3; i++ {
		pdIPAddr, err = findValidPD(pdIPAddress)
		if err == nil && pdIPAddr != "" {
			return pdIPAddr, nil
		}
		logrus.Errorf("find pd address error:%v", err)
		time.Sleep(500 * time.Millisecond)
	}
	return pdIPAddr, err
}

func findValidPD(pdIPAddress []string) (string, error) {
	// 找出有效的pd address
	wg := &sync.WaitGroup{}
	var pdIPAddr string
	tempCtx, tempCancel := context.WithTimeout(context.Background(), 5*time.Second)
	pdAuthority := PDAuthority{}
	tempCtx = pdAuthority.SetAuthority(tempCtx)
	for _, addr := range pdIPAddress {
		wg.Add(1)
		go func(ctx context.Context, cancel context.CancelFunc, addr string) {
			defer wg.Done()
			var md metadata.MD
			pdConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				logrus.Errorf("grpc dial error:%v", err)
				return
			}
			pdClient := pd.NewPDClient(pdConn)
			_, err = pdClient.GetMembers(ctx, &pd.GetMembersRequest{}, grpc.Header(&md))
			pdAuthority.SetToken(md)
			_ = pdConn.Close()
			if err == nil {
				pdIPAddr = addr
				cancel()
			}
		}(tempCtx, tempCancel, addr)
	}
	wg.Wait()
	tempCancel()
	if len(pdIPAddr) == 0 {
		logrus.Errorf("hugegraph.pd_address unable to connect :%v", pdIPAddress)
		return "", errors.New("hugegraph.pd_address unable to connect")
	}
	return pdIPAddr, nil
}

func FindServerAddr(pdIPAddress string, hgName string, username string, password string) (string, error) {
	// retry 3 times to find valid server address
	var serverAddr string
	var err error
	for i := 0; i < 3; i++ {
		serverAddr, err = findServerAddr(pdIPAddress, hgName, username, password)
		if err == nil && serverAddr != "" {
			return serverAddr, nil
		}
		logrus.Errorf("find server address error:%v", err)
		time.Sleep(500 * time.Millisecond)
	}
	return serverAddr, err
}

func findServerAddr(pdIPAddress string, hgName string, username string, password string) (string, error) {
	// 找出有效的server address
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pdConn, err := grpc.Dial(pdIPAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Errorf("dial pd error:%v", err)
		return "", err
	}
	defer pdConn.Close()
	pdAuthority := PDAuthority{}
	ctx = pdAuthority.SetAuthority(ctx)
	pdClient := pd.NewDiscoveryServiceClient(pdConn)
	resp, err := pdClient.GetNodes(ctx, &pd.Query{})
	if err != nil {
		return "", err
	}

	sameSpaceServer := make([]string, 0)
	defaultSpaceServer := make([]string, 0)
	otherServer := make([]string, 0)
	hgSpace, hGraph, err := SplitHgName(hgName)
	if err != nil {
		return "", err
	}
	for _, info := range resp.GetInfo() {
		if info.GetLabels()["GRAPHSPACE"] == hgSpace {
			sameSpaceServer = append(sameSpaceServer, info.Address)
		} else if info.GetLabels()["GRAPHSPACE"] == "DEFAULT" {
			defaultSpaceServer = append(defaultSpaceServer, info.Address)
		} else {
			otherServer = append(otherServer, info.Address)
		}
	}
	// 先校验图空间名字对应的server地址
	validServerAddr := testServerIsValid(sameSpaceServer, hgSpace, hGraph, username, password)
	// 其次再校验默认图空间
	if len(validServerAddr) == 0 {
		validServerAddr = testServerIsValid(defaultSpaceServer, hgSpace, hGraph, username, password)
	}
	// 最后校验其他server是否可用
	if len(validServerAddr) == 0 {
		validServerAddr = testServerIsValid(otherServer, hgSpace, hGraph, username, password)
	}
	if len(validServerAddr) == 0 {
		logrus.Errorf("hugegraph server address unable to connect :%v,%v,%v", sameSpaceServer, defaultSpaceServer, otherServer)
		return "", errors.New("hugegraph server address unable to connect")
	}
	return validServerAddr, nil
}

func testServerIsValid(serverAdds []string, hgSpace, hGraph, username, password string) string {
	tempCtx, tempCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tempCancel()
	var validServerAddr string
	wg := &sync.WaitGroup{}
	for _, addr := range serverAdds {
		wg.Add(1)
		go func(addr string, ctx context.Context, cancel context.CancelFunc) {
			defer wg.Done()
			url := fmt.Sprintf("%v/graphspaces/%v/graphs/%v/schema?format=json", addr, hgSpace, hGraph)
			req, err := http.NewRequest(http.MethodGet, url, nil)
			req.SetBasicAuth(username, password)
			if err != nil {
				return
			}
			req.WithContext(ctx)
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				validServerAddr = addr
				cancel()
				return
			}
		}(addr, tempCtx, tempCancel)
	}
	wg.Wait()
	return validServerAddr
}

func GetHugegraphSchema(serverAddr, hgName, username, password string) (map[string]any, error) {
	hgSpace, hGraph, err := SplitHgName(hgName)
	if err != nil {
		logrus.Errorf("split hg name failed, %v", err.Error())
		return nil, fmt.Errorf("split hg name failed, %v", err.Error())
	}
	url := fmt.Sprintf("%v/graphspaces/%v/graphs/%v/schema?format=json", serverAddr, hgSpace, hGraph)
	// retry 3 times
	var propertyFromHg map[string]any
	for i := 0; i < 3; i++ {
		propertyFromHg, err = getHugegraphSchema(url, username, password)
		if err == nil {
			return propertyFromHg, nil
		}
		logrus.Errorf("get schema failed, %v", err.Error())
		time.Sleep(500 * time.Millisecond)
	}
	return nil, err
}

func getHugegraphSchema(url, username, password string) (map[string]any, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	req.SetBasicAuth(username, password)
	if err != nil {
		logrus.Errorf("create http request failed, %v", err.Error())
		return nil, fmt.Errorf("create http request failed, %v", err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req.WithContext(ctx)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logrus.Errorf("http request failed, %v", err.Error())
		return nil, fmt.Errorf("http request failed, %v", err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed, %v", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		logrus.Errorf("response status not 200 OK, Get:%v", resp.Status)
		return nil, fmt.Errorf("get schema failed, status code:%v, body:%v", resp.StatusCode, string(body))
	}
	propertyFromHg := make(map[string]any)
	err = json.Unmarshal(body, &propertyFromHg)
	if err != nil {
		logrus.Errorf("unmarshal response body failed, %v", err.Error())
		return nil, fmt.Errorf("unmarshal response body failed, %v", err.Error())
	}
	return propertyFromHg, nil
}

func SplitHgName(hgName string) (string, string, error) {
	hgSplit := strings.Split(hgName, "/")
	if len(hgSplit) != 3 {
		return "", "", fmt.Errorf("hugegraph name not correct:%v", hgName)
	}
	return hgSplit[0], hgSplit[1], nil
}

func VariantToInt(variant *hstore.Variant) (int32, error) {
	switch *variant.Type {
	case hstore.VariantType_VT_INT:
		return variant.GetValueInt32(), nil
	case hstore.VariantType_VT_LONG:
		return int32(variant.GetValueInt64()), nil
	}
	return 0, fmt.Errorf("hstore variant type wrong :%v", *variant.Type)
}

func VariantToFloat(variant *hstore.Variant) (float32, error) {
	switch *variant.Type {
	case hstore.VariantType_VT_FLOAT:
		return variant.GetValueFloat(), nil
	case hstore.VariantType_VT_DOUBLE:
		return float32(variant.GetValueDouble()), nil
	}
	return 0, fmt.Errorf("hstore variant type wrong :%v", *variant.Type)
}

func VariantToString(variant *hstore.Variant) (string, error) {
	switch *variant.Type {
	case hstore.VariantType_VT_BOOLEAN:
		if variant.GetValueBoolean() {
			return "true", nil
		}
		return "false", nil
	case hstore.VariantType_VT_INT:
		return strconv.FormatInt(int64(variant.GetValueInt32()), 10), nil
	case hstore.VariantType_VT_LONG:
		return strconv.FormatInt(variant.GetValueInt64(), 10), nil
	case hstore.VariantType_VT_FLOAT:
		return strconv.FormatFloat(float64(variant.GetValueFloat()), 'E', -1, 32), nil
	case hstore.VariantType_VT_DOUBLE:
		return strconv.FormatFloat(variant.GetValueDouble(), 'E', -1, 64), nil
	case hstore.VariantType_VT_STRING:
		return variant.GetValueString(), nil
	case hstore.VariantType_VT_BYTES:
		return string(variant.GetValueBytes()), nil
	case hstore.VariantType_VT_DATETIME:
		return variant.GetValueDatetime(), nil
	}
	return "", fmt.Errorf("hstore variant type wrong :%v", *variant.Type)
}

const (
	CredentialKey   = "credential"
	CredentialValue = "dmVybWVlcjokMmEkMDQkTjg5cUhlMHY1anFOSktoUVpIblRkT0ZTR21pTm9pQTJCMmZkV3BWMkJ3cnRKSzcyZFhZRC4="
	TokenKey        = "Pd-Token"
)

type PDAuthority struct {
	token  string
	locker sync.RWMutex
}

func (pa *PDAuthority) SetAuthority(ctx context.Context) context.Context {
	pa.locker.RLock()
	defer pa.locker.RUnlock()
	md := metadata.New(map[string]string{
		CredentialKey: CredentialValue,
	})
	if pa.token != "" {
		md.Set(TokenKey, pa.token)
	}
	// logrus.Infof("send md:%v", md)
	ctx = metadata.NewOutgoingContext(ctx, md)
	return ctx
}

func (pa *PDAuthority) SetToken(md metadata.MD) {
	pa.locker.Lock()
	defer pa.locker.Unlock()
	// logrus.Infof("recv md:%v", md)
	token := md.Get(TokenKey)
	if len(token) == 1 {
		pa.token = token[0]
	}
}
