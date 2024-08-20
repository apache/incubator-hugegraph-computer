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
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	"github.com/fabric8io/kubernetes-client/generator/pkg/schemagen"
	operatorv1 "hugegraph.apache.org/operator/api/v1"
	machinery "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	packageToken   = "io.fabric8.xxxx"
	javaPackage    = "org.apache.hugegraph.computer.k8s.crd.model"
	goPackage      = "hugegraph.apache.org/operator/api/v1"
	schemaFilePath = "../../computer-k8s/schema/crd-schema.json"
)

func main() {

	// the CRD List types for which the model should be generated
	// no other types need to be defined as they are auto discovered
	crdLists := map[reflect.Type]schemagen.CrdScope{
		// v1
		reflect.TypeOf(operatorv1.HugeGraphComputerJobList{}): schemagen.Namespaced,
	}

	// constraints and patterns for fields
	constraints := map[reflect.Type]map[string]*schemagen.Constraint{}

	// types that are manually defined in the model
	providedTypes := []schemagen.ProvidedType{}

	// go packages that are provided and where no
	// generation is required and their corresponding java package
	providedPackages := map[string]string{
		// external
		"k8s.io/api/core/v1":                   "io.fabric8.kubernetes.api.model",
		"k8s.io/apimachinery/pkg/apis/meta/v1": "io.fabric8.kubernetes.api.model",
		"k8s.io/apimachinery/pkg/api/resource": "io.fabric8.kubernetes.api.model",
		"k8s.io/apimachinery/pkg/runtime":      "io.fabric8.kubernetes.api.model.runtime",
	}

	// mapping of go packages of this module to the resulting java package
	// optional ApiGroup and ApiVersion for the go package
	// (which is added to the generated java class)
	packageMapping := map[string]schemagen.PackageInformation{
		// v1
		goPackage: {
			JavaPackage: packageToken,
			ApiGroup:    operatorv1.GroupVersion.Group,
			ApiVersion:  operatorv1.GroupVersion.Version,
		},
	}

	// converts all packages starting with <key> to
	mappingSchema := map[string]string{}

	// overwriting some times
	manualTypeMap := map[reflect.Type]string{
		reflect.TypeOf(machinery.Time{}):       "java.lang.String",
		reflect.TypeOf(runtime.RawExtension{}): "java.util.Map<String, Object>",
		reflect.TypeOf([]byte{}):               "java.lang.String",
	}

	json := schemagen.GenerateSchema(
		"http://fabric8.io/hugegraph-computer/ComputerSchema#",
		crdLists, providedPackages, manualTypeMap, packageMapping,
		mappingSchema, providedTypes, constraints,
	)

	// fix must start with "io.fabric8."
	json = strings.ReplaceAll(json, packageToken, javaPackage)

	err := ioutil.WriteFile(schemaFilePath, []byte(json+"\n"), 0644)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
