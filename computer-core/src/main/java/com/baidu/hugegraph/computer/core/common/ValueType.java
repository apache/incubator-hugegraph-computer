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

package com.baidu.hugegraph.computer.core.common;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.exception.ComputerException;

public enum ValueType {

    NULL((byte) 1, 0),
    LONG((byte) 2, 8),
    DOUBLE((byte) 3, 8),
    LONG_ID((byte) -1, 8),
    UTF8_ID((byte) -2, -1);

    private static Map<Byte, ValueType> values = new HashMap<>();

    static {
        for (ValueType valueType : ValueType.values()) {
            values.put(valueType.code, valueType);
        }
    }

    private byte code;
    // Is it a fixed value type, -1 means not fixed.
    private int byteSize;

    ValueType(byte code, int byteSize) {
        this.code = code;
        this.byteSize = byteSize;
    }

    public boolean isId() {
        return this.code < 0;
    }

    public static Value createValue(ValueType type) {
        switch (type) {
            case NULL:
                return NullValue.get();
            case LONG_ID:
                return new LongId();
            case LONG:
                return new LongValue();
            case DOUBLE:
                return new DoubleValue();
            case UTF8_ID:
                return new Utf8Id();
            default:
                String message = String.format("Can not create Value for %s.",
                                               type.name());
                throw new RuntimeException(message);
        }
    }

    public static ValueType fromCode(byte code) {
        ValueType valueType = values.get(code);
        if (valueType == null) {
            String message = String.format("Can not find ValueType for code " +
                                           "%s.", code);
            throw new ComputerException(message);
        }
        return valueType;
    }

    public byte code() {
        return this.code;
    }

    public int byteSize() {
        return this.byteSize;
    }
}
