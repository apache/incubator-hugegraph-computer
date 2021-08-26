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

package com.baidu.hugegraph.computer.algorithm.rings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.ListValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.google.common.collect.ImmutableMap;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

public class SpreadFilter {

    private static final String ALL = "*";
    private static final String DEGREE = "D";
    private static final String CURRENT = "C";
    private static final List<Expression> PASS = new ArrayList<>();

    private final Map<String, Expression> vertexFilter;
    private final Map<String, Expression> edgeFilter;

    public SpreadFilter(Config config) {
        String describe = config.get(
                                 ComputerOptions.RINGS_DETECTION_FILTER);
        FilterDescribe filter = FilterDescribe.of(describe);

        this.vertexFilter = new HashMap<>();
        this.edgeFilter = new HashMap<>();

        this.init(this.vertexFilter, filter.vertexFilter());
        this.init(this.edgeFilter, filter.edgeFilter());
    }

    private void init(Map<String, Expression> filter,
                      List<FilterDescribe.DescribeItem> describes) {
        for (FilterDescribe.DescribeItem describe : describes) {
            String labelName = describe.labelName();
            Expression expression = AviatorEvaluator.compile(
                                                     describe.propertyFilter());
            filter.put(labelName, expression);
        }
    }

    public boolean filter(Vertex vertex) {
        String label = vertex.label();
        List<Expression> expressions = getExpression(this.vertexFilter, label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params = ImmutableMap.of(CURRENT,
                                                     vertex.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge) {
        String label = edge.label();
        List<Expression> expressions = getExpression(this.edgeFilter, label);

        if (expressions == PASS) {
            return true;
        }

        expressions = expressions.stream()
                                 .filter(expression -> {
                                     return !expression.getVariableNames()
                                                       .contains(DEGREE);
                                 })
                                 .collect(Collectors.toList());

        Map<String, Map<String, Value<?>>> params = ImmutableMap.of(CURRENT,
                                                    edge.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge, RingsDetectionValue message) {
        String label = edge.label();
        List<Expression> expressions = getExpression(this.edgeFilter, label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value<?>>> params =
                    ImmutableMap.of(CURRENT, edge.properties().get(),
                                    DEGREE, message.degreeEdgeProp().get());
        return filter(params, expressions);
    }

    private static boolean filter(Map<String, Map<String, Value<?>>> params,
                                  List<Expression> expressions) {
        Map<String, Object> map = convert(params);
        return expressions.stream()
                          .allMatch(expression -> {
                              return (Boolean) expression.execute(map);
                          });
    }

    private static List<Expression> getExpression(
            Map<String, Expression> filter,
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

    private static Map<String, Object> convert(
                   Map<String, Map<String, Value<?>>> params) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Value<?>>> entry :
                                                      params.entrySet()) {
            Map<String, Object> subKv = new HashMap<>();
            Map<String, Value<?>> param = entry.getValue();
            for (Map.Entry<String, Value<?>> paramItem : param.entrySet()) {
                subKv.put(paramItem.getKey(), getValue(paramItem.getValue()));
            }
            result.put(entry.getKey(), subKv);
        }
        return result;
    }

    private static Object getValue(Value<?> value) {
        ValueType type = value.type();
        switch (type) {
            case BOOLEAN:
                return  ((BooleanValue) value).value();
            case INT:
                return ((IntValue)value).value();
            case LONG:
                return ((LongValue)value).value();
            case FLOAT:
                return ((FloatValue)value).value();
            case DOUBLE:
                return ((DoubleValue)value).value();
            case LIST_VALUE:
                return ((ListValue<?>)value).values();
            default:
                throw new ComputerException("Can't convert property " +
                                            "value from valueType: %s",
                                            type.name());
        }
    }
}
