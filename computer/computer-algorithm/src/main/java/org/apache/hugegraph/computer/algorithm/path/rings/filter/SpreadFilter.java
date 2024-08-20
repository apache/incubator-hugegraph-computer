/* * Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.hugegraph.computer.algorithm.path.rings.filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.mvel2.MVEL;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SpreadFilter {

    private static final String ALL = "*";
    private static final String MESSAGE = "$message";
    private static final String ELEMENT = "$element";
    private static final List<Serializable> PASS = ImmutableList.of();

    private final Map<String, Serializable> vertexFilter;
    private final Map<String, Serializable> edgeSpreadFilter;
    private final Map<String, Serializable> edgeFilter;

    public SpreadFilter(String describe) {
        FilterDescribe des = FilterDescribe.of(describe);

        this.vertexFilter = new HashMap<>();
        this.edgeSpreadFilter = new HashMap<>();
        this.edgeFilter = new HashMap<>();

        this.init(this.vertexFilter, des.vertexFilter(), expression -> true);
        this.init(this.edgeSpreadFilter, des.edgeFilter(), expression -> true);
        // TODO: Use a better scheme to parse expressions with only $element
        this.init(this.edgeFilter, des.edgeFilter(), expression -> {
            return !expression.contains(MESSAGE);
        });
    }

    private void init(Map<String, Serializable> filter,
                      List<FilterDescribe.DescribeItem> describes,
                      Predicate<? super String> predicate) {
        for (FilterDescribe.DescribeItem describe : describes) {
            String labelName = describe.label();
            String propertyFilter = describe.propertyFilter();
            if (predicate.test(propertyFilter) || ALL.equals(labelName)) {
                Serializable expression = MVEL.compileExpression(
                                               describe.propertyFilter());
                filter.put(labelName, expression);
            }
        }
    }

    public boolean filter(Vertex vertex) {
        String label = vertex.label();
        List<Serializable> expressions = expressions(this.vertexFilter, label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value>> params =
                    ImmutableMap.of(ELEMENT, vertex.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge) {
        String label = edge.label();
        List<Serializable> expressions = expressions(this.edgeFilter, label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value>> params =
                    ImmutableMap.of(ELEMENT, edge.properties().get());
        return filter(params, expressions);
    }

    public boolean filter(Edge edge, RingsDetectionMessage message) {
        String label = edge.label();
        List<Serializable> expressions = expressions(this.edgeSpreadFilter,
                                                     label);

        if (expressions == PASS) {
            return true;
        }

        Map<String, Map<String, Value>> params =
                    ImmutableMap.of(ELEMENT, edge.properties().get(),
                                    MESSAGE, message.walkEdgeProp().get());
        return filter(params, expressions);
    }

    private static boolean filter(Map<String, Map<String, Value>> params,
                                  List<Serializable> expressions) {
        Map<String, Object> map = convertParamsValueToObject(params);
        return expressions.stream()
                          .allMatch(expression -> {
                              return (Boolean) MVEL.executeExpression(
                                                    expression, map);
                          });
    }

    private static List<Serializable> expressions(
                                      Map<String, Serializable> filter,
                                      String label) {
        if (filter.size() == 0) {
            return PASS;
        }
        List<Serializable> expressions = new ArrayList<>();
        if (filter.containsKey(ALL)) {
            expressions.add(filter.get(ALL));
        }
        if (filter.containsKey(label)) {
            expressions.add(filter.get(label));
        }

        return expressions;
    }

    private static Map<String, Object> convertParamsValueToObject(
                   Map<String, Map<String, Value>> params) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Value>> entry : params.entrySet()) {
            Map<String, Object> subKv = new HashMap<>();
            Map<String, Value> param = entry.getValue();
            for (Map.Entry<String, Value> paramItem : param.entrySet()) {
                subKv.put(paramItem.getKey(), paramItem.getValue().value());
            }
            result.put(entry.getKey(), subKv);
        }
        return result;
    }
}
