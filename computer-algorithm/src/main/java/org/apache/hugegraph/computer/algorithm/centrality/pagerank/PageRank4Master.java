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

import org.apache.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import org.apache.hugegraph.computer.core.combiner.LongValueSumCombiner;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.master.MasterComputation;
import org.apache.hugegraph.computer.core.master.MasterComputationContext;
import org.apache.hugegraph.computer.core.master.MasterContext;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class PageRank4Master implements MasterComputation {

    private static final Logger LOG = Log.logger(PageRank4Master.class);

    public static final String CONF_L1_NORM_DIFFERENCE_THRESHOLD_KEY =
                               "pagerank.l1DiffThreshold";
    public static final double CONF_L1_DIFF_THRESHOLD_DEFAULT = 0.00001D;

    public static final String AGGR_L1_NORM_DIFFERENCE_KEY =
                               "pagerank.aggr_l1_norm_difference";
    public static final String AGGR_DANGLING_VERTICES_NUM =
                               "pagerank.dangling_vertices_num";
    public static final String AGGR_COMULATIVE_DANGLING_PROBABILITY =
                               "pagerank.comulative_dangling_probability";
    public static final String AGGR_COMULATIVE_PROBABILITY =
                               "pagerank.comulative_probability";

    private double l1DiffThreshold;

    @Override
    public void init(MasterContext context) {
        this.l1DiffThreshold = context.config().getDouble(
                               CONF_L1_NORM_DIFFERENCE_THRESHOLD_KEY,
                               CONF_L1_DIFF_THRESHOLD_DEFAULT);
        context.registerAggregator(AGGR_DANGLING_VERTICES_NUM,
                                   ValueType.LONG,
                                   LongValueSumCombiner.class);
        context.registerAggregator(AGGR_COMULATIVE_DANGLING_PROBABILITY,
                                   ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
        context.registerAggregator(AGGR_COMULATIVE_PROBABILITY,
                                   ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
        context.registerAggregator(AGGR_L1_NORM_DIFFERENCE_KEY,
                                   ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
    }

    @Override
    public void close(MasterContext context) {
        // pass
    }

    @Override
    public boolean compute(MasterComputationContext context) {
        LongValue danglingVerticesNum = context.aggregatedValue(
                                        AGGR_DANGLING_VERTICES_NUM);
        DoubleValue danglingProbability = context.aggregatedValue(
                                          AGGR_COMULATIVE_DANGLING_PROBABILITY);
        DoubleValue cumulativeProbability = context.aggregatedValue(
                                            AGGR_COMULATIVE_PROBABILITY);
        DoubleValue l1NormDifference = context.aggregatedValue(
                                       AGGR_L1_NORM_DIFFERENCE_KEY);

        StringBuilder sb = new StringBuilder();
        sb.append("[Superstep ").append(context.superstep()).append("]")
          .append(", dangling vertices num = ").append(danglingVerticesNum)
          .append(", cumulative dangling probability = ")
          .append(danglingProbability.value())
          .append(", cumulative probability = ").append(cumulativeProbability)
          .append(", l1 norm difference = ").append(l1NormDifference.value());

        LOG.info("PageRank running status: {}", sb);
        double l1Diff = l1NormDifference.value();
        if (context.superstep() > 1 && l1Diff <= this.l1DiffThreshold) {
            return false;
        } else {
            return true;
        }
    }
}
