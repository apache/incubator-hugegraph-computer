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

package org.apache.hugegraph.computer.algorithm.centrality.pagerank;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.aggregator.Aggregator;
import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.computer.core.worker.WorkerContext;

public class PageRank implements Computation<DoubleValue> {

    public static final String OPTION_ALPHA = "page_rank.alpha";

    public static final double ALPHA_DEFAULT_VALUE = 0.15;

    private double alpha;
    private double danglingRank;
    private double initialRankInSuperstep;
    private double cumulativeRank;

    private Aggregator<DoubleValue> l1DiffAggr;
    private Aggregator<DoubleValue> cumulativeRankAggr;
    private Aggregator<LongValue> danglingVertexNumAggr;
    private Aggregator<DoubleValue> danglingCumulativeAggr;

    // Initial value in superstep 0.
    private DoubleValue initialValue;
    private DoubleValue contribValue;

    @Override
    public String name() {
        return "page_rank";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(this.initialValue);
        this.cumulativeRankAggr.aggregateValue(this.initialValue.value());
        int edgeCount = vertex.numEdges();
        if (edgeCount == 0) {
            this.danglingVertexNumAggr.aggregateValue(1L);
            this.danglingCumulativeAggr.aggregateValue(
                                        this.initialValue.value());
        } else {
            this.contribValue.value(this.initialValue.value() / edgeCount);
            context.sendMessageToAllEdges(vertex, this.contribValue);
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<DoubleValue> messages) {
        DoubleValue message = Combiner.combineAll(context.combiner(), messages);
        double rankFromNeighbors = 0.0;
        if (message != null) {
            rankFromNeighbors = message.value();
        }
        double rank = (this.danglingRank + rankFromNeighbors) *
                      (1.0 - this.alpha) + this.initialRankInSuperstep;
        rank /= this.cumulativeRank;
        DoubleValue oldRank = vertex.value();
        vertex.value(new DoubleValue(rank));
        this.l1DiffAggr.aggregateValue(Math.abs(oldRank.value() - rank));
        this.cumulativeRankAggr.aggregateValue(rank);
        int edgeCount = vertex.numEdges();
        if (edgeCount == 0) {
            this.danglingVertexNumAggr.aggregateValue(1L);
            this.danglingCumulativeAggr.aggregateValue(rank);
        } else {
            this.contribValue.value(rank / edgeCount);
            context.sendMessageToAllEdges(vertex, this.contribValue);
        }
    }

    @Override
    public void init(Config config) {
        this.alpha = config.getDouble(OPTION_ALPHA, ALPHA_DEFAULT_VALUE);
        this.contribValue = new DoubleValue();
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        // Get aggregator values for computation
        DoubleValue danglingTotalRank = context.aggregatedValue(
                    PageRank4Master.AGGR_COMULATIVE_DANGLING_PROBABILITY);
        DoubleValue cumulativeRank = context.aggregatedValue(
                    PageRank4Master.AGGR_COMULATIVE_PROBABILITY);
        long totalVertex = context.totalVertexCount();

        this.danglingRank = danglingTotalRank.value() / totalVertex;
        this.initialRankInSuperstep = this.alpha / totalVertex;
        this.cumulativeRank = cumulativeRank.value();
        this.initialValue = new DoubleValue(1.0 / totalVertex);

        // Create aggregators
        this.l1DiffAggr = context.createAggregator(
                          PageRank4Master.AGGR_L1_NORM_DIFFERENCE_KEY);
        this.cumulativeRankAggr = context.createAggregator(
                                  PageRank4Master.AGGR_COMULATIVE_PROBABILITY);
        this.danglingVertexNumAggr = context.createAggregator(
             PageRank4Master.AGGR_DANGLING_VERTICES_NUM);
        this.danglingCumulativeAggr = context.createAggregator(
             PageRank4Master.AGGR_COMULATIVE_DANGLING_PROBABILITY);
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        context.aggregateValue(
                PageRank4Master.AGGR_COMULATIVE_PROBABILITY,
                this.cumulativeRankAggr.aggregatedValue());
        context.aggregateValue(
                PageRank4Master.AGGR_L1_NORM_DIFFERENCE_KEY,
                this.l1DiffAggr.aggregatedValue());
        context.aggregateValue(
                PageRank4Master.AGGR_DANGLING_VERTICES_NUM,
                this.danglingVertexNumAggr.aggregatedValue());
        context.aggregateValue(
                PageRank4Master.AGGR_COMULATIVE_DANGLING_PROBABILITY,
                this.danglingCumulativeAggr.aggregatedValue());
    }
}
