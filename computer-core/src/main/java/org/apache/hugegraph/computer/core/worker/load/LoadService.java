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

package org.apache.hugegraph.computer.core.worker.load;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.vertex.DefaultVertex;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.input.EdgeFetcher;
import org.apache.hugegraph.computer.core.input.GraphFetcher;
import org.apache.hugegraph.computer.core.input.HugeConverter;
import org.apache.hugegraph.computer.core.input.InputFilter;
import org.apache.hugegraph.computer.core.input.InputSourceFactory;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.computer.core.input.VertexFetcher;
import org.apache.hugegraph.computer.core.rpc.InputSplitRpcService;
import org.apache.hugegraph.util.E;

public class LoadService {

    private final GraphFactory graphFactory;
    private final Config config;

    // Service proxy on the client
    private InputSplitRpcService rpcService;
    private final InputFilter inputFilter;

    public LoadService(ComputerContext context) {
        this.graphFactory = context.graphFactory();
        this.config = context.config();
        this.rpcService = null;
        this.inputFilter = context.config().createObject(
                ComputerOptions.INPUT_FILTER_CLASS);
    }

    public void init() {
        assert this.rpcService != null;
    }

    public void rpcService(InputSplitRpcService rpcService) {
        E.checkNotNull(rpcService, "rpcService");
        this.rpcService = rpcService;
    }

    public Iterator<Vertex> createIteratorFromVertex() {
        GraphFetcher fetcher = InputSourceFactory.createGraphFetcher(this.config, this.rpcService);
        return new IteratorFromVertex(fetcher);
    }

    public Iterator<Vertex> createIteratorFromEdge() {
        GraphFetcher fetcher = InputSourceFactory.createGraphFetcher(this.config, this.rpcService);
        return new IteratorFromEdge(fetcher);
    }

    private class IteratorFromVertex implements Iterator<Vertex>, AutoCloseable {

        private InputSplit currentSplit;

        // GraphFetcher includes VertexFetcher
        private GraphFetcher fetcher;

        public IteratorFromVertex(GraphFetcher fetcher) {
            this.currentSplit = null;
            this.fetcher = fetcher;
        }

        @Override
        public boolean hasNext() {
            VertexFetcher vertexFetcher = this.fetcher.vertexFetcher();
            while (this.currentSplit == null || !vertexFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.currentSplit = this.fetcher.nextVertexInputSplit();
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
            org.apache.hugegraph.structure.graph.Vertex hugeVertex;
            hugeVertex = this.fetcher.vertexFetcher().next();
            return this.convert(hugeVertex);
        }

        private Vertex convert(org.apache.hugegraph.structure.graph.Vertex
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

        @Override
        public void close() throws IOException {
            this.fetcher.close();
        }
    }

    private class IteratorFromEdge implements Iterator<Vertex>, AutoCloseable {

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

        // GraphFetcher includes EdgeFetcher
        private GraphFetcher fetcher;

        public IteratorFromEdge(GraphFetcher fetcher) {
            // this.direction = config.get(ComputerOptions.EDGE_DIRECTION);
            this.maxEdges = config.get(
                            ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
            this.currentSplit = null;
            this.currentVertex = null;
            this.fetcher = fetcher;
        }

        @Override
        public boolean hasNext() {
            if (InputSplit.END_SPLIT.equals(this.currentSplit)) {
                return this.currentVertex != null;
            }
            EdgeFetcher edgeFetcher = this.fetcher.edgeFetcher();
            while (this.currentSplit == null || !edgeFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.currentSplit = this.fetcher.nextEdgeInputSplit();
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

            org.apache.hugegraph.structure.graph.Edge hugeEdge;
            EdgeFetcher edgeFetcher = this.fetcher.edgeFetcher();
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

        private Edge convert(org.apache.hugegraph.structure.graph.Edge edge) {
            edge = inputFilter.filter(edge);
            Id targetId = HugeConverter.convertId(edge.targetId());
            Properties properties = HugeConverter.convertProperties(
                                    edge.properties());
            Edge computerEdge = graphFactory.createEdge(edge.label(),
                                                        edge.name(), targetId
            );
            computerEdge.label(edge.label());
            computerEdge.properties(properties);
            return computerEdge;
        }

        @Override
        public void close() throws IOException {
            this.fetcher.close();
        }
    }
}
