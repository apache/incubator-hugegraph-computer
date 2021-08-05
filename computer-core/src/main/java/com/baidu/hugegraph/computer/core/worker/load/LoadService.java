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

package com.baidu.hugegraph.computer.core.worker.load;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.HugeConverter;
import com.baidu.hugegraph.computer.core.input.InputFilter;
import com.baidu.hugegraph.computer.core.input.InputSourceFactory;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.util.E;

public class LoadService {

    private final GraphFactory graphFactory;
    private final Config config;
    /*
     * GraphFetcher include:
     *   VertexFetcher vertexFetcher;
     *   EdgeFetcher edgeFetcher;
     */
    private GraphFetcher fetcher;
    // Service proxy on the client
    private InputSplitRpcService rpcService;
    private final InputFilter inputFilter;

    public LoadService(ComputerContext context) {
        this.graphFactory = context.graphFactory();
        this.config = context.config();
        this.fetcher = null;
        this.rpcService = null;
        this.inputFilter = context.config().createObject(
                           ComputerOptions.INPUT_FILTER_CLASS);
    }

    public void init() {
        assert this.rpcService != null;
        this.fetcher = InputSourceFactory.createGraphFetcher(this.config,
                                                             this.rpcService);
    }

    public void close() {
        this.fetcher.close();
    }

    public void rpcService(InputSplitRpcService rpcService) {
        E.checkNotNull(rpcService, "rpcService");
        this.rpcService = rpcService;
    }

    public Iterator<Vertex> createIteratorFromVertex() {
        return new IteratorFromVertex();
    }

    public Iterator<Vertex> createIteratorFromEdge() {
        return new IteratorFromEdge();
    }

    private class IteratorFromVertex implements Iterator<Vertex> {

        private InputSplit currentSplit;

        public IteratorFromVertex() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            VertexFetcher vertexFetcher = fetcher.vertexFetcher();
            while (this.currentSplit == null || !vertexFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.currentSplit = fetcher.nextVertexInputSplit();
                if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                    return false;
                }
                vertexFetcher.prepareLoadInputSplit(this.currentSplit);
            }
            return true;
        }

        @Override
        public Vertex next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            com.baidu.hugegraph.structure.graph.Vertex hugeVertex;
            hugeVertex = fetcher.vertexFetcher().next();
            return this.convert(hugeVertex);
        }

        private Vertex convert(com.baidu.hugegraph.structure.graph.Vertex
                               vertex) {
            vertex = inputFilter.filter(vertex);
            Id id = HugeConverter.convertId(vertex.id());
            String label = vertex.label();
            Properties properties = HugeConverter.convertProperties(
                                                  vertex.properties());
            Vertex computerVertex = graphFactory.createVertex(label, id, null);
            computerVertex.properties(properties);
            return computerVertex;
        }
    }

    private class IteratorFromEdge implements Iterator<Vertex> {

        /*
         * TODO: If it is an in edge, we should get the data from the in shard;
         * if it is a both edge, should get the data from the out, and then
         * convert each edge to two vertices. For the time being, only consider
         * the case of the out edge.
         */
        // private final Direction direction;
        private final int maxEdges;
        private InputSplit currentSplit;
        private Vertex currentVertex;

        public IteratorFromEdge() {
            // this.direction = config.get(ComputerOptions.EDGE_DIRECTION);
            this.maxEdges = config.get(
                            ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
            this.currentSplit = null;
            this.currentVertex = null;
        }

        @Override
        public boolean hasNext() {
            if (InputSplit.END_SPLIT.equals(this.currentSplit)) {
                return this.currentVertex != null;
            }
            EdgeFetcher edgeFetcher = fetcher.edgeFetcher();
            while (this.currentSplit == null || !edgeFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.currentSplit = fetcher.nextEdgeInputSplit();
                if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                    return this.currentVertex != null;
                }
                edgeFetcher.prepareLoadInputSplit(this.currentSplit);
            }
            return true;
        }

        @Override
        public Vertex next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            com.baidu.hugegraph.structure.graph.Edge hugeEdge;
            EdgeFetcher edgeFetcher = fetcher.edgeFetcher();
            while (edgeFetcher.hasNext()) {
                hugeEdge = edgeFetcher.next();
                Edge edge = this.convert(hugeEdge);
                Id sourceId = HugeConverter.convertId(hugeEdge.sourceId());
                if (this.currentVertex == null) {
                    this.currentVertex = new DefaultVertex(graphFactory,
                                                           sourceId, null);
                    this.currentVertex.addEdge(edge);
                } else if (this.currentVertex.id().equals(sourceId) &&
                           this.currentVertex.numEdges() < this.maxEdges) {
                    /*
                     * Current edge is the adjacent edge of previous vertex and
                     * not reached the threshold of one vertex can hold
                     */
                    this.currentVertex.addEdge(edge);
                } else {
                    /*
                     * Current edge isn't the adjacent edge of previous vertex
                     * or reached the threshold of one vertex can hold
                     */
                    Vertex vertex = this.currentVertex;
                    this.currentVertex = new DefaultVertex(graphFactory,
                                                           sourceId, null);
                    this.currentVertex.addEdge(edge);
                    return vertex;
                }
            }
            assert this.currentVertex != null;
            Vertex vertex = this.currentVertex;
            this.currentVertex = null;
            return vertex;
        }

        private Edge convert(com.baidu.hugegraph.structure.graph.Edge edge) {
            edge = inputFilter.filter(edge);
            Id targetId = HugeConverter.convertId(edge.targetId());
            Properties properties = HugeConverter.convertProperties(
                                    edge.properties());
            Edge computerEdge = graphFactory.createEdge(edge.label(),
                                                        edge.name(), targetId
            );
            computerEdge.properties(properties);
            return computerEdge;
        }
    }
}
