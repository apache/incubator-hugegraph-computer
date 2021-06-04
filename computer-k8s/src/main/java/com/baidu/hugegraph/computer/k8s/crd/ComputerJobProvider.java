/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.k8s.crd;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;

import io.fabric8.kubernetes.api.KubernetesResourceMappingProvider;
import io.fabric8.kubernetes.api.model.KubernetesResource;

public class ComputerJobProvider implements KubernetesResourceMappingProvider {

    public static final String VERSION = "v1";
    public static final String GROUP = "computer.hugegraph.io";
    public static final String KIND = "HugeGraphComputerJob";
    public static final String PLURAL = "hugegraphcomputerjobs";
    public static final String SINGULAR = "hugegraphcomputerjob";
    public static final String API_VERSION = GROUP + "/" + VERSION;

    public final Map<String, Class<? extends KubernetesResource>> mappings =
           new HashMap<>();

    public ComputerJobProvider() {
        mappings.put("computer.hugegraph.io/v1#HugeGraphComputerJob",
                     HugeGraphComputerJob.class);
    }

    @Override
    public Map<String, Class<? extends KubernetesResource>> getMappings() {
        return mappings;
    }
}
