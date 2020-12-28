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

public enum ValueType {

    NULL_VALUE(0),
    LONG_VALUE(8),
    DOUBLE_VALUE(8),
    LONG_ID(8),
    TEXT_ID(-1);

    private static ValueType[] values = ValueType.values();

    // Is it a fixed value type, -1 means not fixed.
    private int byteSize;

    ValueType(int byteSize) {
        this.byteSize = byteSize;
    }

    public static Value createValue(ValueType type) {
        switch (type) {
            case NULL_VALUE:
                return NullValue.get();
            case LONG_ID:
                return new LongId();
            case LONG_VALUE:
                return new LongValue();
            case DOUBLE_VALUE:
                return new DoubleValue();
            case TEXT_ID:
                return new TextId();
            default:
                String message = String.format("Can not create Value for %s.",
                                               type.name());
                throw new RuntimeException(message);
        }
    }

    public static ValueType fromOrdinal(int ordinal) {
        return values[ordinal];
    }

    public int byteSize() {
        return this.byteSize;
    }
}
