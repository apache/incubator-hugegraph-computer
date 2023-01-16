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

package org.apache.hugegraph.computer.core.bsp;

public enum BspEvent {

    // The WORKER or MASTER after "BSP_" indicates who set the flag.
    BSP_MASTER_INIT_DONE(1, "/master/init"),
    BSP_WORKER_INIT_DONE(2, "/worker/init"),
    BSP_MASTER_ALL_INIT_DONE(3, "/master/all_init"),
    BSP_MASTER_RESUME_DONE(4, "/master/resume"),
    BSP_WORKER_INPUT_DONE(5, "/worker/input"),
    BSP_MASTER_INPUT_DONE(6, "/master/input"),
    BSP_WORKER_STEP_PREPARE_DONE(7, "/worker/step_prepare"),
    BSP_MASTER_STEP_PREPARE_DONE(8, "/master/step_prepare"),
    BSP_WORKER_STEP_COMPUTE_DONE(9, "/worker/step_compute"),
    BSP_MASTER_STEP_COMPUTE_DONE(10, "/master/step_compute"),
    BSP_WORKER_STEP_DONE(11, "/worker/step_done"),
    BSP_MASTER_STEP_DONE(12, "/master/step_done"),
    BSP_WORKER_OUTPUT_DONE(13, "/worker/output_done"),
    BSP_WORKER_CLOSE_DONE(14, "/worker/close_done");

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
        return this.key;
    }
}
