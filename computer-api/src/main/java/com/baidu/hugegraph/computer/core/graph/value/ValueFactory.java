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

package com.baidu.hugegraph.computer.core.graph.value;

import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;

public final class ValueFactory {

    public static Value<?> createValue(byte code) {
        ValueType type = SerialEnum.fromCode(ValueType.class, code);
        return createValue(type);
    }

    // Can reuse Value
    public static Value<?> createValue(ValueType type) {
        switch (type) {
            case NULL:
                return NullValue.get();
            case BOOLEAN:
                return new BooleanValue();
            case INT:
                return new IntValue();
            case LONG:
                return new LongValue();
            case FLOAT:
                return new FloatValue();
            case DOUBLE:
                return new DoubleValue();
            case ID_VALUE:
                return new IdValue();
            case ID_VALUE_LIST:
                return new IdValueList();
            case ID_VALUE_LIST_LIST:
                return new IdValueListList();
            case LIST_VALUE:
                return new ListValue<>();
            default:
                throw new ComputerException("Can't create Value for %s",
                                            type.name());
        }
    }
}
