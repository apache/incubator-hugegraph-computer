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

package com.baidu.hugegraph.computer.algorithm;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.googlecode.aviator.Expression;

public class ExpressionUtil {

    public static Map<String, Object> convertParamsValueToObject(
                  Map<String, Map<String, Value<?>>> params) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Value<?>>> entry :
                params.entrySet()) {
            Map<String, Object> subKv = new HashMap<>();
            Map<String, Value<?>> param = entry.getValue();
            for (Map.Entry<String, Value<?>> paramItem : param.entrySet()) {
                subKv.put(paramItem.getKey(), paramItem.getValue().value());
            }
            result.put(entry.getKey(), subKv);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> T expressionExecute(
                        Map<String, Map<String, Value<?>>> param,
                        Expression expression) {
        return (T) expression.execute(
               ExpressionUtil.convertParamsValueToObject(param));
    }
}
