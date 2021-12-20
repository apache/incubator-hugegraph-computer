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
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.algorithm.ExpressionUtil;
import com.baidu.hugegraph.computer.algorithm.path.filter.PropertyFilterDescribe;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.google.common.collect.ImmutableMap;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

public class RingsDetectionSpreadFilter {

    private static final String ALL = "*";
    private static final String MESSAGE = "$message";
    private static final String ELEMENT = "$element";

    private final Map<String, Expression> vertexFilter;
    private final Map<String, Expression> edgeFilter;

    public RingsDetectionSpreadFilter(String describe) {
        RingsDetectionFilterDescribe filter =
                                     RingsDetectionFilterDescribe.of(describe);

        this.vertexFilter = new HashMap<>();
        this.edgeFilter = new HashMap<>();

        this.init(this.vertexFilter, filter.vertexFilter());
        this.init(this.edgeFilter, filter.edgeFilter());
    }

    private void init(Map<String, Expression> filter,
                      List<PropertyFilterDescribe> describes) {
        if (CollectionUtils.isEmpty(describes)) {
            filter.put(ALL, null);
            return;
        }
        for (PropertyFilterDescribe describe : describes) {
            String labelName = describe.label();
            Expression expression = null;
            if (describe.propertyFilter() != null) {
                expression = AviatorEvaluator.compile(
                                              describe.propertyFilter());
            }
            filter.put(labelName, expression);
        }
    }

    public boolean filter(Vertex vertex) {
        if (isLabelCannotSpread(this.vertexFilter, vertex.label())) {
            return false;
        }

        List<Expression> expressions = expressions(this.vertexFilter,
                                                   vertex.label(),
                                                   expression -> true);
        if (CollectionUtils.isEmpty(expressions)) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(ELEMENT, vertex.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge) {
        if (isLabelCannotSpread(this.edgeFilter, edge.label())) {
            return false;
        }

        List<Expression> expressions =
                         expressions(this.edgeFilter,
                                     edge.label(),
                                     expression -> {
                                         return !expression.getVariableNames()
                                                           .contains(MESSAGE);
                                     });
        if (CollectionUtils.isEmpty(expressions)) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(ELEMENT, edge.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge, RingsDetectionMessage message) {
        if (isLabelCannotSpread(this.edgeFilter, edge.label())) {
            return false;
        }

        List<Expression> expressions = expressions(this.edgeFilter,
                                                   edge.label(),
                                                   expression -> true);
        if (CollectionUtils.isEmpty(expressions)) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(ELEMENT, edge.properties().get(),
                                    MESSAGE, message.walkEdgeProp().get());
        return filter(params, expressions);
    }

    private static boolean filter(Map<String, Map<String, Value<?>>> params,
                                  List<Expression> expressions) {
        Map<String, Object> map =
                            ExpressionUtil.convertParamsValueToObject(params);
        return expressions.stream()
                          .allMatch(expression -> {
                              return (Boolean) expression.execute(map);
                          });
    }

    private static List<Expression> expressions(
                   Map<String, Expression> filter, String label,
                   Predicate<? super Expression> expressionFilter) {
        List<Expression> expressions = new ArrayList<>();
        if (filter.containsKey(ALL)) {
            expressions.add(filter.get(ALL));
        }
        if (filter.containsKey(label)) {
            expressions.add(filter.get(label));
        }

        return expressions.stream()
                          .filter(Objects::nonNull)
                          .filter(expressionFilter)
                          .collect(Collectors.toList());
    }

    private static boolean isLabelCannotSpread(Map<String, Expression> filter,
                                               String label) {
        return !filter.containsKey(ALL) && !filter.containsKey(label);
    }
}
