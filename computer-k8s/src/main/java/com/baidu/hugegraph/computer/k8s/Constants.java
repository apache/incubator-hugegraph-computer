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

package com.baidu.hugegraph.computer.k8s;

public class Constants {

    public static final int MASTER_INSTANCES = 1;

    public static final int TOTAL_COMPONENTS = 2;
    public static final int ALLOW_FAILED_JOBS = 0;
    public static final int ALLOW_FAILED_COMPONENTS = 0;

    public static final String COMPONENT_STATE_NOT_READY = "NotReady";
    public static final String COMPONENT_STATE_READY = "Ready";
    public static final String COMPONENT_STATE_DELETED = "Deleted";

    public static final String POD_REASON_UNSCHEDULABLE = "Unschedulable";
}
