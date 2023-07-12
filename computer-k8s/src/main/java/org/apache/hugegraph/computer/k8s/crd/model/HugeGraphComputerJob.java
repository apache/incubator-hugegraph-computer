/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.k8s.crd.model;

import java.util.Map;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(HugeGraphComputerJob.VERSION)
@Group(HugeGraphComputerJob.GROUP)
@Kind(HugeGraphComputerJob.KIND)
@Plural(HugeGraphComputerJob.PLURAL)
@Singular(HugeGraphComputerJob.SINGULAR)
public class HugeGraphComputerJob
       extends CustomResource<ComputerJobSpec, ComputerJobStatus>
       implements Namespaced {

    private static final long serialVersionUID = 6788317713035067450L;

    public static final String VERSION = "v1";
    public static final String GROUP = "operator.hugegraph.apache.org";
    public static final String KIND = "HugeGraphComputerJob";
    public static final String PLURAL = "hugegraphcomputerjobs";
    public static final String SINGULAR = "hugegraphcomputerjob";
    public static final String API_VERSION = GROUP + "/" + VERSION;

    public static ComputerJobSpec mapToSpec(Map<String, Object> specMap) {
        String yaml = Serialization.asYaml(specMap);
        return Serialization.unmarshal(yaml, ComputerJobSpec.class);
    }

    public static Map<String, Object> specToMap(ComputerJobSpec spec) {
        String yaml  = Serialization.asYaml(spec);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = Serialization.unmarshal(yaml, Map.class);
        return map;
    }
}
