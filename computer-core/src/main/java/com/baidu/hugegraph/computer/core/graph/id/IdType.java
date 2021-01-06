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

package com.baidu.hugegraph.computer.core.graph.id;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.core.exception.ComputerException;

public enum IdType {

    LONG(1),
    UTF8(2),
    UUID(3);

    private static Map<Byte, IdType> values = new HashMap<>();

    static {
        for (IdType valueType : IdType.values()) {
            values.put(valueType.code, valueType);
        }
    }

    private final byte code;

    IdType(int code) {
        assert code < 256;
        this.code = (byte) code;
    }

    public byte code() {
        return this.code;
    }

    // Maybe can reuse Id
    public static Id createID(byte code) {
        IdType type = fromCode(code);
        return createID(type);
    }

    public static Id createID(IdType type) {
        switch (type) {
            case LONG:
                return new LongId();
            case UTF8:
                return new Utf8Id();
            case UUID:
                return new UuidId();
            default:
                throw new ComputerException("Can't create Id for %s.",
                                            type.name());
        }
    }

    public static IdType fromCode(byte code) {
        IdType valueType = values.get(code);
        if (valueType == null) {
            throw new ComputerException("Can't find IdType for code %s.",
                                        code);
        }
        return valueType;
    }
}
