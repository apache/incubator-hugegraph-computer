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

package com.baidu.hugegraph.computer.algorithm.path.rings.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.baidu.hugegraph.computer.algorithm.path.filter.FilterDescribe;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

public class SpreadFilter {

    private static final String ALL = "*";
    private static final String MESSAGE = "$message";
    private static final String ELEMENT = "$element";
    private static final List<Expression> PASS = ImmutableList.of();

    private final Map<String, Expression> vertexFilter;
    private final Map<String, Expression> edgeFilter;

    public SpreadFilter(String describe) {
        RingsDetectionFilterDescribe filter =
                                     RingsDetectionFilterDescribe.of(describe);

        this.vertexFilter = new HashMap<>();
        this.edgeFilter = new HashMap<>();

        this.init(this.vertexFilter, filter.vertexFilter());
        this.init(this.edgeFilter, filter.edgeFilter());
    }

    private void init(Map<String, Expression> filter,
                      List<FilterDescribe> describes) {
        for (FilterDescribe describe : describes) {
            String labelName = describe.label();
            Expression expression = AviatorEvaluator.compile(
                                                     describe.propertyFilter());
            filter.put(labelName, expression);
        }
    }

    public boolean filter(Vertex vertex) {
        String label = vertex.label();
        List<Expression> expressions = expressions(this.vertexFilter, label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(ELEMENT, vertex.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge) {
        String label = edge.label();
        List<Expression> expressions = expressions(this.edgeFilter, label);

        if (expressions == PASS) {
            return true;
        }

        expressions = expressions.stream()
                                 .filter(expression -> {
                                     return !expression.getVariableNames()
                                                       .contains(MESSAGE);
                                 })
                                 .collect(Collectors.toList());

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(ELEMENT, edge.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge, RingsDetectionMessage message) {
        String label = edge.label();
        List<Expression> expressions = expressions(this.edgeFilter, label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(ELEMENT, edge.properties().get(),
                                    MESSAGE, message.walkEdgeProp().get());
        return filter(params, expressions);
    }

    private static boolean filter(Map<String, Map<String, Value<?>>> params,
                                  List<Expression> expressions) {
        Map<String, Object> map = convertParamsValueToObject(params);
        return expressions.stream()
                          .allMatch(expression -> {
                              return (Boolean) expression.execute(map);
                          });
    }

    private static List<Expression> expressions(Map<String, Expression> filter,
                                                String label) {
        if (filter.size() == 0) {
            return PASS;
        }
        List<Expression> expressions = new ArrayList<>();
        if (filter.containsKey(ALL)) {
            expressions.add(filter.get(ALL));
        }
        if (filter.containsKey(label)) {
            expressions.add(filter.get(label));
        }

        return expressions;
    }

    private static Map<String, Object> convertParamsValueToObject(
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
}
