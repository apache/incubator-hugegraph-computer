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

package threshold

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"vermeer/apps/common"
	"vermeer/apps/storage"

	"github.com/sirupsen/logrus"
)

var store storage.Store
var storeType storage.StoreType = storage.StoreTypePebble
var storeLock sync.Mutex

// assure must to done without any error
type assure func()

func initStore() {
	p, err := common.GetCurrentPath()

	if err != nil {
		panic(fmt.Errorf("failed to get current path error: %w", err))
	}

	dir := path.Join(p, "vermeer_data", "threshold_info")

	store, err = storage.StoreMaker(storage.StoreOption{
		StoreName: storage.StoreTypePebble,
		Path:      dir,
		Fsync:     true,
	})
	if err != nil {
		panic(fmt.Errorf("failed to initialize the kv store, caused by: %w. maybe another vermeer master is running", err))
	}
}

func makeData(prefix string, key string, value any) (data map[string]any) {
	data = make(map[string]any, 0)
	putData(data, prefix, key, value)
	return
}

func putData(data map[string]any, prefix string, key string, value any) {
	data[strings.Join([]string{prefix, key}, " ")] = value
}

func batchSave(data map[string]any, process []assure) error {
	if err := atomProcess(func() error { return saveObjects(data) }, process); err != nil {
		return err
	}

	return nil
}

func atomProcess(atom func() error, process []assure) (err error) {
	if err = atom(); err == nil {
		for _, p := range process {
			p()
		}
	}
	return err
}

func saveObjects(kv map[string]any) error {
	storeLock.Lock()
	defer storeLock.Unlock()

	batch := store.NewBatch()

	for k, v := range kv {
		var bytes []byte
		var err error

		if bytes, err = convertToBytes(v); err != nil {
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

func retrieveData(prefix string, valueType reflect.Kind) map[string]any {
	storeLock.Lock()
	defer storeLock.Unlock()

	dataMap := make(map[string]any)
	dataKeys := make([]string, 0)

	for kv := range store.Scan() {
		if strings.HasPrefix(string(kv.Key), prefix) {
			dataKeys = append(dataKeys, string(kv.Key))
			continue
		}
	}

	for _, s := range dataKeys {
		if buf, err := store.Get([]byte(s)); err == nil {
			v := string(buf)

			if v == "" {
				logrus.Warnf("data restoration was aborted due to an empty value, key: %s", s)
				continue
			}

			keys := strings.Split(s, " ")
			keylen := len(keys)

			if keylen < 2 {
				logrus.Errorf("data restoration was aborted due to an illegal length of keys, length: %d, keys: %s", keylen, keys)
				continue
			}

			dataMap[keys[1]], err = convertToType(v, valueType)

			if err != nil {
				logrus.Errorf("failed to convert data value type to %v: %v", valueType, err)
				continue
			}

			logrus.Infof("retrieved a data entry from store, keys: %s, value: %s", keys, v)
		}

	}

	return dataMap
}

// onvertToBytes Conversion function, supports all primitive types.
func convertToBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return []byte(fmt.Sprintf("%v", v)), nil
	case uint, uint8, uint16, uint32, uint64:
		return []byte(fmt.Sprintf("%v", v)), nil
	case float32:
		return []byte(fmt.Sprintf("%f", v)), nil
	case float64:
		return []byte(fmt.Sprintf("%f", v)), nil
	case bool:
		return []byte(fmt.Sprintf("%v", v)), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported value type: %s", reflect.TypeOf(value).String())
	}
}

// convertToType Conversion function, supports all primitive types.
func convertToType(value string, targetType reflect.Kind) (interface{}, error) {
	switch targetType {
	case reflect.Int:
		return strconv.Atoi(value)
	case reflect.Int8:
		intValue, err := strconv.ParseInt(value, 10, 8)
		return int8(intValue), err
	case reflect.Int16:
		intValue, err := strconv.ParseInt(value, 10, 16)
		return int16(intValue), err
	case reflect.Int32:
		intValue, err := strconv.ParseInt(value, 10, 32)
		return int32(intValue), err
	case reflect.Int64:
		return strconv.ParseInt(value, 10, 64)
	case reflect.Uint:
		uintValue, err := strconv.ParseUint(value, 10, 64)
		return uint(uintValue), err
	case reflect.Uint8:
		uintValue, err := strconv.ParseUint(value, 10, 8)
		return uint8(uintValue), err
	case reflect.Uint16:
		uintValue, err := strconv.ParseUint(value, 10, 16)
		return uint16(uintValue), err
	case reflect.Uint32:
		uintValue, err := strconv.ParseUint(value, 10, 32)
		return uint32(uintValue), err
	case reflect.Uint64:
		return strconv.ParseUint(value, 10, 64)
	case reflect.Float32:
		return strconv.ParseFloat(value, 32)
	case reflect.Float64:
		return strconv.ParseFloat(value, 64)
	case reflect.Bool:
		return strconv.ParseBool(value)
	case reflect.String:
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported target type: %s", targetType.String())
	}
}
