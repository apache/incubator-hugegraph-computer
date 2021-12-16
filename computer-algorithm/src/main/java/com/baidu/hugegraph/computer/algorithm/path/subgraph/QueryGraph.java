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

package com.baidu.hugegraph.computer.algorithm.path.subgraph;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.algorithm.ExpressionUtil;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.googlecode.aviator.Expression;

public class QueryGraph {

    private final Map<String, Vertex> vertexIdMap;
    private final BitSet edgeVisited;

    public QueryGraph(String graphDescribe) {
        List<QueryGraphDescribe.VertexDescribe> describes =
                                QueryGraphDescribe.of(graphDescribe);
        E.checkArgument(CollectionUtils.isNotEmpty(describes),
                        "Query graph of subgraph match must be have at " +
                        "least one vertex");

        this.vertexIdMap = new HashMap<>();
        Map<Integer, Edge> edgeMap = new HashMap<>();
        int edgeId = 0;
        for (QueryGraphDescribe.VertexDescribe describe : describes) {
            String vId = describe.id();
            Vertex vertex = new Vertex(vId, describe.label(),
                                       describe.propertyFilter());
            this.vertexIdMap.put(vertex.id(), vertex);
            // Init out-edge
            List<QueryGraphDescribe.EdgeDescribe> edgeDescribes =
                                                  describe.edges();
            for (QueryGraphDescribe.EdgeDescribe edgeDescribe : edgeDescribes) {
                Edge edge = new Edge(edgeId++, vertex.id(),
                                     edgeDescribe.targetId(),
                                     edgeDescribe.label(),
                                     edgeDescribe.propertyFilter());
                vertex.addOutEdge(edge);
                edgeMap.put(edgeId, edge);
            }
        }

        Collection<Edge> edges = edgeMap.values();
        for (Edge edge : edges) {
            Vertex vertex = this.vertexIdMap.get(edge.target);
            if (vertex == null) {
                throw new ComputerException("Can't find vertex [%s] in query " +
                                            "graph config", edge.target);
            }
            vertex.addInEdge(edge);
        }
        this.edgeVisited = new BitSet(edges.size());
    }

    public List<Vertex> vertices() {
        Vertex[] vertices = this.vertexIdMap.values().toArray(new Vertex[0]);
        return ImmutableList.copyOf(vertices);
    }

    public Vertex findVertexById(String id) {
        return this.vertexIdMap.get(id);
    }

    public void resetEdgeVisited() {
        this.edgeVisited.clear();
    }

    public void visitEdge(int edgeId) {
        this.edgeVisited.set(edgeId);
    }

    public boolean isEdgeVisited(int edgeId) {
        return this.edgeVisited.get(edgeId);
    }

    public static class Vertex {

        private final String id;
        private final String label;
        private final Expression propertyFilter;
        private final List<Edge> inEdges;
        private final List<Edge> outEdges;

        private Vertex(String id, String label, Expression propertyFilter) {
            this.id = id;
            this.label = label;
            this.propertyFilter = propertyFilter;
            this.inEdges = new ArrayList<>();
            this.outEdges = new ArrayList<>();
        }

        public String id() {
            return this.id;
        }

        public String label() {
            return this.label;
        }

        public List<Edge> inEdges() {
            return this.inEdges;
        }

        public List<Edge> outEdges() {
            return this.outEdges;
        }

        public void addInEdge(Edge edge) {
            this.inEdges.add(edge);
        }

        public void addOutEdge(Edge edge) {
            this.outEdges.add(edge);
        }

        public boolean match(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            if (!this.label.equals(vertex.label())) {
                return false;
            }
            if (this.propertyFilter == null) {
                return true;
            }

            Map<String, Map<String, Value<?>>> param =
                    ImmutableMap.of("$element", vertex.properties().get());
            return ExpressionUtil.expressionExecute(param, this.propertyFilter);
        }
    }

    public static class Edge {

        private final int id;
        private final String source;
        private final String target;
        private final String label;
        private final Expression propertyFilter;

        public Edge(int id, String source, String target, String label,
                    Expression propertyFilter) {
            this.id = id;
            this.source = source;
            this.target = target;
            this.label = label;
            this.propertyFilter = propertyFilter;
        }

        public int id() {
            return this.id;
        }

        public String source() {
            return this.source;
        }

        public String target() {
            return this.target;
        }

        public String label() {
            return this.label;
        }

        public boolean match(com.baidu.hugegraph.computer.core.graph.edge.Edge
                             edge) {
            if (!this.label.equals(edge.label())) {
                return false;
            }
            if (this.propertyFilter == null) {
                return true;
            }
            Map<String, Map<String, Value<?>>> param =
                ImmutableMap.of("$element", edge.properties().get());
            return ExpressionUtil.expressionExecute(param, this.propertyFilter);
        }
    }
}
