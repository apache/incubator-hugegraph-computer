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

package org.apache.hugegraph.computer.algorithm.centrality.degree;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.NullValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.computer.core.worker.WorkerContext;
import org.apache.hugegraph.util.NumericUtil;

public class DegreeCentrality implements Computation<NullValue> {

    public static final String OPTION_WEIGHT_PROPERTY = "degree_centrality.weight_property";

    private boolean calculateByWeightProperty;
    private String weightProperty;

    @Override
    public String name() {
        return "degree_centrality";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        if (!this.calculateByWeightProperty) {
            vertex.value(new DoubleValue(vertex.numEdges()));
        } else {
            /*
             *  TODO: Here we use doubleValue type now, we will use BigDecimal
             *  and output "BigDecimalValue" to resolve double type overflow
             *  int the future;
             */
            double totalWeight = 0.0;
            for (Edge edge : vertex.edges()) {
                double weight = weightValue(edge.property(this.weightProperty));
                totalWeight += weight;
                if (Double.isInfinite(totalWeight)) {
                    throw new ComputerException("Calculate weight overflow, " +
                                                "current is %s, edge '%s' " +
                                                "is %s", totalWeight, edge, weight);
                }
            }
            vertex.value(new DoubleValue(totalWeight));
        }
        vertex.inactivate();
    }

    private static double weightValue(Value value) {
        if (value == null) {
            return 1.0;
        }

        switch (value.valueType()) {
            case LONG:
            case INT:
            case DOUBLE:
            case FLOAT:
                return NumericUtil.convertToNumber(value).doubleValue();
            default:
                throw new ComputerException("The weight property can only be " +
                                            "either Long or Int or Double or " +
                                            "Float, but got %s",
                                            value.valueType());
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<NullValue> messages) {
        // pass
    }

    @Override
    public void init(Config config) {
        this.weightProperty = config.getString(
                              OPTION_WEIGHT_PROPERTY, "");
        this.calculateByWeightProperty = StringUtils.isNotEmpty(
                                         this.weightProperty);
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        // pass
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        // pass
    }
}
