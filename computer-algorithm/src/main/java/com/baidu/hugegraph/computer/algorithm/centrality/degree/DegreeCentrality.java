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

package com.baidu.hugegraph.computer.algorithm.centrality.degree;

import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.NullValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;
import com.baidu.hugegraph.util.E;

public class DegreeCentrality implements Computation<NullValue> {

    public static final String CONF_DEGREE_CENTRALITY_WEIGHT_PROPERTY =
                               "degree.centrality.weight.property";
    private boolean calculateByWeight;
    private String weight;

    @Override
    public String name() {
        return "degreeCentrality";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        if (!this.calculateByWeight) {
            vertex.value(new DoubleValue(vertex.numEdges()));
        } else {
            Edge edge = null;
            BigDecimal totalWeight = BigDecimal.valueOf(0.0);
            Iterator<Edge> edges = vertex.edges().iterator();
            while (edges.hasNext()) {
                edge = edges.next();
                DoubleValue value = edge.properties().get(this.weight);
                E.checkArgumentNotNull(value, "The edge's '%s' weight " +
                                              "property '%s' can't be null",
                                       edge.name(), this.weight);
                totalWeight = totalWeight.add(
                              BigDecimal.valueOf(value.value()));
            }
            vertex.value(new DoubleValue(totalWeight.doubleValue()));
        }
        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<NullValue> messages) {
        // pass
    }

    @Override
    public void init(Config config) {
        this.weight = config.getString(CONF_DEGREE_CENTRALITY_WEIGHT_PROPERTY,
                                       "");
        this.calculateByWeight = StringUtils.isNotEmpty(this.weight);
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
