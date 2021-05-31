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

import com.baidu.hugegraph.computer.k8s.crd.models.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.models.ComputerJobStatus;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(ComputerJob.VERSION)
@Group(ComputerJob.GROUP)
@Kind(ComputerJob.KIND)
@Plural(ComputerJob.PLURAL)
@Singular(ComputerJob.SINGULAR)
public class ComputerJob
       extends CustomResource<ComputerJobSpec, ComputerJobStatus>
       implements Namespaced {

    public static final String VERSION = "v1";
    public static final String GROUP = "computer.hugegraph.io";
    public static final String KIND = "HugeGraphComputerJob";
    public static final String PLURAL = "HugeGraphComputerJobs";
    public static final String SINGULAR = "HugeGraphComputerJob";
}
