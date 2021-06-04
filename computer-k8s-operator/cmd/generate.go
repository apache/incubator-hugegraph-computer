/**
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	computerv1 "computer.hugegraph.io/operator/api/v1"
	"github.com/fabric8io/kubernetes-client/generator/pkg/schemagen"
	machinery "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func main() {

	// the CRD List types for which the model should be generated
	// no other types need to be defined as they are auto discovered
	crdLists := map[reflect.Type]schemagen.CrdScope{
		// v1
		reflect.TypeOf(computerv1.HugeGraphComputerJobList{}): schemagen.Namespaced,
	}

	// constraints and patterns for fields
	constraints := map[reflect.Type]map[string]*schemagen.Constraint{}

	// types that are manually defined in the model
	providedTypes := []schemagen.ProvidedType{}

	// go packages that are provided and where no generation is required and their corresponding java package
	providedPackages := map[string]string{
		// external
		"k8s.io/api/core/v1":                   "io.fabric8.kubernetes.api.model",
		"k8s.io/apimachinery/pkg/apis/meta/v1": "io.fabric8.kubernetes.api.model",
		"k8s.io/apimachinery/pkg/api/resource": "io.fabric8.kubernetes.api.model",
		"k8s.io/apimachinery/pkg/runtime":      "io.fabric8.kubernetes.api.model.runtime",
	}

	// mapping of go packages of this module to the resulting java package
	// optional ApiGroup and ApiVersion for the go package (which is added to the generated java class)
	packageMapping := map[string]schemagen.PackageInformation{
		// v1
		"computer.hugegraph.io/operator/api/v1": {
			JavaPackage: "io.fabric8.xxxx",
			ApiGroup:    computerv1.GroupVersion.Group,
			ApiVersion:  computerv1.GroupVersion.Version,
		},
	}

	// converts all packages starting with <key> to
	// a java package using an automated scheme:
	//  - replace <key> with <value> aka "package prefix"
	//  - replace '/' with '.' for a valid java package name
	// e.g. github.com/apache/camel-k/pkg/apis/camel/v1/knative/CamelEnvironment is mapped to "io.fabric8.camelk.internal.pkg.apis.camel.v1.knative.CamelEnvironment"
	mappingSchema := map[string]string{
		//"github.com/apache/camel-k/pkg/apis/camel/v1/knative": "io.fabric8.camelk.v1beta1.internal",
	}

	// overwriting some times
	manualTypeMap := map[reflect.Type]string{
		reflect.TypeOf(machinery.Time{}):       "java.lang.String",
		reflect.TypeOf(runtime.RawExtension{}): "java.util.Map<String, Object>",
		reflect.TypeOf([]byte{}):               "java.lang.String",
	}

	json := schemagen.GenerateSchema(
		"http://fabric8.io/hugegraph-computer/v1/ComputerSchema#",
		crdLists, providedPackages, manualTypeMap, packageMapping,
		mappingSchema, providedTypes, constraints,
	)

	// fix must start with "io.fabric8."
	json = strings.ReplaceAll(json, "io.fabric8.xxxx",
		"com.baidu.hugegraph.computer.k8s.crd.model")

	err := ioutil.WriteFile("./schema/kube-schema.json", []byte(json), 0644)
	if err != nil {
		fmt.Errorf(err.Error())
		os.Exit(1)
	}
}
