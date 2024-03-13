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

package org.apache.hugegraph.computer.core.graph.id;

import static org.apache.hugegraph.computer.algorithm.AlgorithmParams.BYTESID_CLASS_NAME;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;

public final class IdFactory {

    private static final Constructor<?> BYTES_ID_CONSTRUCTOR;
    private static final Method BYTES_ID_LONG_METHOD;
    private static final Method BYTES_ID_STRING_METHOD;
    private static final Method BYTES_ID_UUID_METHOD;

    private static final UUID EMPTY_UUID = new UUID(0L, 0L);

    static {
        try {
            Class<?> bytesIdClass = Class.forName(BYTESID_CLASS_NAME);

            BYTES_ID_CONSTRUCTOR = bytesIdClass.getDeclaredConstructor();
            BYTES_ID_CONSTRUCTOR.setAccessible(true);

            BYTES_ID_LONG_METHOD = bytesIdClass.getMethod("of", long.class);
            BYTES_ID_LONG_METHOD.setAccessible(false);

            BYTES_ID_STRING_METHOD = bytesIdClass.getMethod("of", String.class);
            BYTES_ID_STRING_METHOD.setAccessible(false);

            BYTES_ID_UUID_METHOD = bytesIdClass.getMethod("of", UUID.class);
            BYTES_ID_UUID_METHOD.setAccessible(false);
        } catch (Throwable e) {
            throw new ComputerException("Failed to reflection BytesId method", e);
        }
    }

    // Maybe can reuse Id
    public static Id createId(byte code) {
        IdType type = SerialEnum.fromCode(IdType.class, code);
        return createId(type);
    }

    public static Id createId(IdType type) {
        try {
            switch (type) {
                case LONG:
                    return (Id) BYTES_ID_LONG_METHOD.invoke(null, 0L);
                case UTF8:
                    return (Id) BYTES_ID_STRING_METHOD.invoke(null, Constants.EMPTY_STR);
                case UUID:
                    return (Id) BYTES_ID_UUID_METHOD.invoke(null, EMPTY_UUID);
                default:
                    throw new ComputerException("Can't create Id for %s",
                                                type.name());
            }
        } catch (Exception e) {
            throw new ComputerException("Failed to createId", e);
        }
    }

    public static Id parseId(IdType type, Object value) {
        try {
            switch (type) {
                case LONG:
                    return (Id) BYTES_ID_LONG_METHOD.invoke(null, value);
                case UTF8:
                    return (Id) BYTES_ID_STRING_METHOD.invoke(null, value);
                case UUID:
                    return (Id) BYTES_ID_UUID_METHOD.invoke(null, value);
                default:
                    throw new ComputerException("Unexpected id type %s", type.name());
            }
        } catch (Exception e) {
            throw new ComputerException("Failed to parse %s Id: '%s'", e, type, value);
        }
    }

    public static Id createId() {
        try {
            return (Id) BYTES_ID_CONSTRUCTOR.newInstance();
        } catch (Exception e) {
            throw new ComputerException("Can't create Id for %s");
        }
    }
}
