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

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.util.E;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.setVisibility(PropertyAccessor.FIELD,
                                    JsonAutoDetect.Visibility.ANY);
    }

    public static String toJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new ComputerException("Can't write json: %s",
                                        e, e.getMessage());
        }
    }

    public static String toJsonWithClass(Object obj) {
        E.checkArgumentNotNull(obj, "The obj can't be null");
        return obj.getClass().getSimpleName() + toJson(obj);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        E.checkState(json != null,
                     "Json value can't be null for '%s'",
                     clazz.getSimpleName());
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new ComputerException("Can't read json: %s",
                                        e, e.getMessage());
        }
    }
}
