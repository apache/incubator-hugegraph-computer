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

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.exception.ComputerException;

public enum ValueType {

    NULL(1, 0),
    LONG(2, 8),
    DOUBLE(3, 8),
    ID_VALUE(128, -1);

    private static Map<Integer, ValueType> values = new HashMap<>();

    static {
        for (ValueType valueType : ValueType.values()) {
            values.put(valueType.code, valueType);
        }
    }

    private int code;
    // Is it a fixed value type, -1 means not fixed.
    private int byteSize;

    ValueType(int code, int byteSize) {
        this.code = code;
        this.byteSize = byteSize;
    }

    public boolean isId() {
        return this.code >= 128;
    }

    public static Value createValue(byte code) {
        ValueType type = fromCode(code);
        return createValue(type);
    }

    public static Value createValue(ValueType type) {
        switch (type) {
            case NULL:
                return NullValue.get();
            case LONG:
                return new LongValue();
            case DOUBLE:
                return new DoubleValue();
            case ID_VALUE:
                return new IdValue();
            default:
                String message = "Can't create Value for %s.";
                throw new ComputerException(message, type.name());
        }
    }

    public static ValueType fromCode(int code) {
        ValueType valueType = values.get(code);
        if (valueType == null) {
            String message = "Can't find ValueType for code %s.";
            throw new ComputerException(message, code);
        }
        return valueType;
    }

    public int code() {
        return this.code;
    }

    public int byteSize() {
        return this.byteSize;
    }
}
