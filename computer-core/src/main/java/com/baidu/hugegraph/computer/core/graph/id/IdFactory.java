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

import java.util.UUID;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;

public final class IdFactory {

    // Maybe can reuse Id
    public static Id createId(byte code) {
        IdType type = SerialEnum.fromCode(IdType.class, code);
        return createId(type);
    }

    public static Id createId(IdType type) {
        switch (type) {
            case LONG:
                return BytesId.of(0L);
            case UTF8:
                return BytesId.of(Constants.EMPTY_STR);
            case UUID:
                return BytesId.of(new UUID(0L, 0L));
            default:
                throw new ComputerException("Can't create Id for %s",
                                            type.name());
        }
    }
}
