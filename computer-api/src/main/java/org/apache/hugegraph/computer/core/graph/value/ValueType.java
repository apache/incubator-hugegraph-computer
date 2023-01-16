/*
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

package org.apache.hugegraph.computer.core.graph.value;

import org.apache.hugegraph.computer.core.common.SerialEnum;

public enum ValueType implements SerialEnum {

    UNKNOWN(-1, -1, "unknown"),
    NULL(1, 0, "null"),
    BOOLEAN(2, 1, "bool"),
    INT(3, 4, "int"),
    LONG(4, 8, "long"),
    FLOAT(5, 4, "float"),
    DOUBLE(6, 8, "double"),
    STRING(7, -1, "string"),
    ID(20, -1, "id"),
    ID_LIST(21, -1, "idList"),
    ID_LIST_LIST(22, -1, "idListList"),
    ID_SET(23, -1, "idSet"),
    LIST_VALUE(30, -1, "list"),
    MAP_VALUE(31, -1, "map"),
    CUSTOMIZE_VALUE(100, -1, "customize");

    private final byte code;
    // Length in bytes if it's a fixed value type, -1 means not fixed.
    private final int byteSize;
    // Name of this value type
    private final String name;

    static {
        SerialEnum.register(ValueType.class);
    }

    ValueType(int code, int byteSize, String name) {
        assert code >= -128 && code <= 127;
        this.code = (byte) code;
        this.byteSize = byteSize;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public int byteSize() {
        return this.byteSize;
    }

    public String string() {
        return this.name;
    }
}
