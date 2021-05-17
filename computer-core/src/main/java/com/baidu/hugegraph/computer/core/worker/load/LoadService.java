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
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.HugeConverter;
import com.baidu.hugegraph.computer.core.input.InputSourceFactory;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.util.E;

public class LoadService {

    private final GraphFactory graphFactory;
    private Config config;
    /*
     * GraphFetcher include:
     *   VertexFetcher vertexFetcher;
     *   EdgeFetcher edgeFetcher;
     */
    private GraphFetcher fetcher;
    // Service proxy on the client
    private InputSplitRpcService rpcService;

    public LoadService(ComputerContext context) {
        this.graphFactory = context.graphFactory();
        this.config = null;
        this.fetcher = null;
        this.rpcService = null;
    }

    public void init(Config config) {
        assert this.rpcService != null;
        this.config = config;
        this.fetcher = InputSourceFactory.createGraphFetcher(config,
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

        private InputSplit inputSplit;

        public IteratorFromVertex() {
            this.inputSplit = null;
        }

        @Override
        public boolean hasNext() {
            VertexFetcher vertexFetcher = fetcher.vertexFetcher();
            if (this.inputSplit == null || !vertexFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.inputSplit = fetcher.nextVertexInputSplit();
                if (this.inputSplit == InputSplit.END_SPLIT) {
                    return false;
                }
                vertexFetcher.prepareLoadInputSplit(this.inputSplit);
            }
            assert this.inputSplit != null &&
                   this.inputSplit != InputSplit.END_SPLIT;
            return vertexFetcher.hasNext();
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
            Id id = HugeConverter.convertId(vertex.id());
            Properties properties = HugeConverter.convertProperties(
                                    vertex.properties());
            Vertex computerVertex = new DefaultVertex(graphFactory, id, null);
            computerVertex.properties(properties);
            return computerVertex;
        }
    }

    private class IteratorFromEdge implements Iterator<Vertex> {

        // TODO: 如果是in边，应该从in的分片中获取数据；如果是both边，应该从out中
        // 获取数据，然后每个edge转换为两个顶点，暂时只考虑out边的情况
        // private final Direction direction;
        private final int maxEdgesInOneVertex;
        private InputSplit inputSplit;
        private Vertex currVertex;

        public IteratorFromEdge() {
            // this.direction = config.get(ComputerOptions.EDGE_DIRECTION);
            this.maxEdgesInOneVertex = config.get(
                                       ComputerOptions.MAX_EDGES_IN_ONE_VERTEX);
            this.inputSplit = null;
            this.currVertex = null;
        }

        @Override
        public boolean hasNext() {
            EdgeFetcher edgeFetcher = fetcher.edgeFetcher();
            if (this.inputSplit == null || !edgeFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.inputSplit = fetcher.nextEdgeInputSplit();
                if (this.inputSplit == InputSplit.END_SPLIT) {
                    return false;
                }
                edgeFetcher.prepareLoadInputSplit(this.inputSplit);
            }
            assert this.inputSplit != null &&
                   this.inputSplit != InputSplit.END_SPLIT;
            return edgeFetcher.hasNext();
        }

        @Override
        public Vertex next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            com.baidu.hugegraph.structure.graph.Edge hugeEdge;
            while (this.hasNext()) {
                hugeEdge = fetcher.edgeFetcher().next();
                Edge edge = this.convert(hugeEdge);
                Id sourceId = HugeConverter.convertId(hugeEdge.sourceId());
                if (this.currVertex == null) {
                    this.currVertex = new DefaultVertex(graphFactory,
                                                        sourceId, null);
                    this.currVertex.addEdge(edge);
                } else if (this.currVertex.id().equals(sourceId) &&
                           this.currVertex.numEdges() <
                           this.maxEdgesInOneVertex) {
                    /*
                     * Current edge is the adjacent edge of previous vertex and
                     * not reached the threshold of one vertex can hold
                     */
                    this.currVertex.addEdge(edge);
                } else {
                    /*
                     * Current edge isn't the adjacent edge of previous vertex
                     * or reached the threshold of one vertex can hold
                     */
                    Vertex vertex = this.currVertex;
                    this.currVertex = new DefaultVertex(graphFactory,
                                                        sourceId, null);
                    this.currVertex.addEdge(edge);
                    return vertex;
                }
            }
            assert this.currVertex != null;
            Vertex vertex = this.currVertex;
            this.currVertex = null;
            return vertex;
        }

        private Edge convert(com.baidu.hugegraph.structure.graph.Edge edge) {
            Id targetId = HugeConverter.convertId(edge.targetId());
            String name = edge.name();
            Properties properties = HugeConverter.convertProperties(
                                    edge.properties());
            Edge computerEdge = new DefaultEdge(graphFactory, targetId,
                                                name, null);
            computerEdge.properties(properties);
            return computerEdge;
        }
    }
}
