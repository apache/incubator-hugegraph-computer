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

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdFactory;
import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.util.JsonUtil;

public class IdUtil {

    public static Id parseId(String idStr) {
        if (StringUtils.isBlank(idStr)) {
            throw new IllegalArgumentException("Can't parse Id for empty string");
        }

        try {
            if (idStr.startsWith("U\"")) {
                return IdFactory.parseId(IdType.UUID,
                                         UUID.fromString(idStr.substring(1)
                                                              .replaceAll("\"", "")));
            }

            Object id = JsonUtil.fromJson(idStr, Object.class);
            idStr = idStr.replaceAll("\"", "");
            return id instanceof Number ?
                   IdFactory.parseId(IdType.LONG, Long.valueOf(idStr)) :
                   IdFactory.parseId(IdType.UTF8, idStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "The vertex id must be formatted as Number/String/UUID" +
                    ", but got '%s'", idStr));
        }
    }
}
