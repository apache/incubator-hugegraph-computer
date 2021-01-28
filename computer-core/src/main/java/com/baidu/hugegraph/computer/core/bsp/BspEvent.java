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

package com.baidu.hugegraph.computer.core.bsp;

public enum BspEvent {

    // The WORKER or MASTER after "BSP_" indicates who set the flag.
    BSP_MASTER_REGISTERED(1, "/master"),
    BSP_WORKER_REGISTERED(2, "/worker"),
    BSP_MASTER_SUPERSTEP_RESUME(3, "/master/superstep_resume"),
    BSP_WORKER_INPUT_DONE(4, "/worker/input_done"),
    BSP_MASTER_INPUT_DONE(5, "/master/input_done"),
    BSP_WORKER_SUPERSTEP_PREPARED(6, "/worker/superstep_prepared"),
    BSP_MASTER_SUPERSTEP_PREPARED(7, "/master/superstep_prepared"),
    BSP_WORKER_SUPERSTEP_DONE(8, "/worker/superstep_done"),
    BSP_MASTER_SUPERSTEP_DONE(9, "/master/superstep_done"),
    BSP_WORKER_OUTPUT_DONE(10, "/worker/output_done");

    private byte code;
    private String key;

    BspEvent(int code, String key) {
        assert code >= -128 && code <= 127;
        this.code = (byte) code;
        this.key = key;
    }

    public byte code() {
        return this.code;
    }

    public String key() {
        return key;
    }
}
