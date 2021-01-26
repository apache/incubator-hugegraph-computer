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

package com.baidu.hugegraph.computer.core.common;

public final class Constants {

    public static final byte[] EMPTY_BYTES = new byte[0];

    public static final int BOOLEAN_LEN = 1;
    public static final int BYTE_LEN = Byte.BYTES;
    public static final int SHORT_LEN = Short.BYTES;
    public static final int INT_LEN = Integer.BYTES;
    public static final int LONG_LEN = Long.BYTES;
    public static final int CHAR_LEN = Character.BYTES;
    public static final int FLOAT_LEN = Float.BYTES;
    public static final int DOUBLE_LEN = Double.BYTES;
    public static final int BLOB_LEN = 4;

    public static final int UINT8_MAX = ((byte) -1) & 0xff;
    public static final int UINT16_MAX = ((short) -1) & 0xffff;
    public static final long UINT32_MAX = (-1) & 0xffffffffL;

    // The WORKER or MASTER after "BSP_" indicates who set the flag.
    public static final String BSP_MASTER_REGISTER_PATH = "/master";
    public static final String BSP_WORKER_REGISTER_PATH = "/worker";
    public static final String BSP_MASTER_FIRST_SUPERSTEP_PATH =
                               "/first-superstep";
    public static final String BSP_WORKER_READ_DONE_PATH = "/worker-read-done";
    public static final String BSP_WORKER_SUPERSTEP_DONE_PATH =
                               "/worker-superstep-done";
    public static final String BSP_MASTER_SUPERSTEP_DONE_PATH =
                               "/master-superstep-done";
    public static final String BSP_WORKER_PREPARE_SUPERSTEP_DONE_PATH =
                               "/worker-superstep-prepare-done";
    public static final String BSP_WORKER_SAVE_DONE_PATH =
                               "/worker-save-done";
}
