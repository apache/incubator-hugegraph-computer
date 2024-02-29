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

package org.apache.hugegraph.computer.core.util;

import java.io.IOException;
import java.util.List;

import org.apache.hugegraph.rest.SerializeException;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

// TODO: move to org.apache.hugegraph.util.JsonUtil later
public class JsonUtilExt {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> List<T> fromJson2List(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, getCollectionType(List.class, clazz));
        } catch (IOException e) {
            throw new SerializeException("Failed to deserialize json '%s'",
                                         e, json);
        }
    }

    private static JavaType getCollectionType(Class<?> collectionClass,
                                              Class<?>... elementClasses) {
        return MAPPER.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }
}
