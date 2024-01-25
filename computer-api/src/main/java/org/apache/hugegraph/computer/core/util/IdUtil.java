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

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdCategory;
import org.apache.hugegraph.computer.core.graph.id.IdFactory;
import org.apache.hugegraph.computer.core.graph.id.IdType;

public class IdUtil {

    private static String UUID_REGEX = "^[0-9a-fA-F]{8}-" +
                                       "[0-9a-fA-F]{4}-" +
                                       "[0-9a-fA-F]{4}-" +
                                       "[0-9a-fA-F]{4}-" +
                                       "[0-9a-fA-F]{12}$";
    private static Pattern P = Pattern.compile(UUID_REGEX);

    public static Id parseId(String idStr) {
        if (StringUtils.isBlank(idStr)) {
            throw new ComputerException("Can't parse Id for empty string");
        }

        if (StringUtils.isNumeric(idStr)) {
            return IdFactory.parseId(IdType.LONG, idStr);
        } else if (P.matcher(idStr).matches()) {
            return IdFactory.parseId(IdType.UUID, idStr);
        } else {
            return IdFactory.parseId(IdType.UTF8, idStr);
        }
    }

    public static Id parseId(IdCategory idCategory, String idStr) {
        if (StringUtils.isBlank(idStr)) {
            throw new ComputerException("Can't parse Id for empty string");
        }

        if (idCategory == null) {
            // automatic inference
            return parseId(idStr);
        }

        switch (idCategory) {
            case NUMBER:
                return IdFactory.parseId(IdType.LONG, idStr);
            case UUID:
                return IdFactory.parseId(IdType.UUID, idStr);
            default:
                return IdFactory.parseId(IdType.UTF8, idStr);
        }
    }
}
