/*
 *
 *  Copyright 2017 HugeGraph Authors
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with this
 *  work for additional information regarding copyright ownership. The ASF
 *  licenses this file to You under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.baidu.hugegraph.computer.core.graph.value;

import com.baidu.hugegraph.computer.core.common.SerialEnum;

public enum ValueType implements SerialEnum {

    UNKNOWN(-1, -1),
    NULL(1, 0),
    INT(2, 4),
    LONG(3, 8),
    FLOAT(4, 4),
    DOUBLE(5, 8),
    ID_VALUE(20, -1),
    ID_VALUE_LIST(30, -1),
    ID_VALUE_LIST_LIST(40, -1),
    LIST_VALUE(80, -1);

    private byte code;
    // Is it a fixed value type, -1 means not fixed.
    private int byteSize;

    static {
        SerialEnum.register(ValueType.class);
    }

    ValueType(int code, int byteSize) {
        assert code >= -128 && code <= 127;
        this.code = (byte) code;
        this.byteSize = byteSize;
    }

    public byte code() {
        return this.code;
    }

    public int byteSize() {
        return this.byteSize;
    }
}
