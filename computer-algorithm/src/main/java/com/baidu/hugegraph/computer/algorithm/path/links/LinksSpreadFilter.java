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

package com.baidu.hugegraph.computer.algorithm.path.links;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.computer.algorithm.ExpressionUtil;
import com.baidu.hugegraph.computer.algorithm.path.filter.PropertyFilterDescribe;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.google.common.collect.ImmutableMap;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

public class LinksSpreadFilter {

    private static final String ELEMENT = "$element";
    private static final String IN = "$in";
    private static final String OUT = "$out";

    private final Set<Id> startVertexes;
    private Pair<String, Expression> endVertexCondition;
    private Pair<String, Expression> endEdgeCondition;
    private final Pair<String, Expression> edgeSpreadCondition;

    public LinksSpreadFilter(String config) {
        LinksConditionDescribe describe = LinksConditionDescribe.of(config);

        this.startVertexes = new HashSet<>();
        describe.startVertexes().stream()
                                .map(BytesId::of)
                                .forEach(this.startVertexes::add);

        Expression expression;

        PropertyFilterDescribe vertexEndCondition =
                               describe.vertexEndCondition();
        if (vertexEndCondition != null) {
            expression = AviatorEvaluator.compile(
                                          vertexEndCondition.propertyFilter());
            this.endVertexCondition = new ImmutablePair<>(
                                          vertexEndCondition.label(),
                                          expression);
        }

        PropertyFilterDescribe edgeEndCondition = describe.edgeEndCondition();
        if (edgeEndCondition != null) {
            expression = AviatorEvaluator.compile(
                                          edgeEndCondition.propertyFilter());
            this.endEdgeCondition = new ImmutablePair<>(
                                        edgeEndCondition.label(), expression);
        }


        PropertyFilterDescribe edgeSpreadCondition =
                               describe.edgeCompareCondition();
        expression = AviatorEvaluator.compile(
                                      edgeSpreadCondition.propertyFilter());
        this.edgeSpreadCondition = new ImmutablePair<>(
                                       edgeSpreadCondition.label(),
                                       expression);
    }

    public boolean isStartVertexes(Vertex vertex) {
        //should use long id as below
        //String strid = Long.toString((long)vertex.id().asObject());
        //Id id = BytesId.of(strid);
        //return this.startVertexes.contains(id);
        return this.startVertexes.contains(vertex.id());
    }

    public boolean isEndVertex(Vertex vertex) {
        if (this.endVertexCondition == null ||
            !this.endVertexCondition.getKey().equals(vertex.label())) {
            return false;
        }

        Map<String, Map<String, Value<?>>> param =
                    ImmutableMap.of(ELEMENT, vertex.properties().get());
        return ExpressionUtil.expressionExecute(
                              param, this.endEdgeCondition.getValue());
    }

    public boolean isEndEdge(Edge edge) {
        if (this.endEdgeCondition == null ||
            !this.endEdgeCondition.getKey().equals(edge.label())) {
            return false;
        }

        Map<String, Map<String, Value<?>>> param =
                    ImmutableMap.of(OUT, edge.properties().get());
        return ExpressionUtil.expressionExecute(
                              param, this.endEdgeCondition.getValue());
    }

    public boolean isEdgeCanSpread0(Edge edge) {
        return this.edgeSpreadCondition.getKey().equals(edge.label());
    }

    public boolean isEdgeCanSpread(Edge edge,
                                   Properties lastEdgeProperties) {
        if (!this.edgeSpreadCondition.getKey().equals(edge.label())) {
            return false;
        }

        Map<String, Map<String, Value<?>>> param =
                    ImmutableMap.of(OUT, edge.properties().get(),
                                    IN, lastEdgeProperties.get());
        return ExpressionUtil.expressionExecute(
                              param, this.edgeSpreadCondition.getValue());
    }
}
