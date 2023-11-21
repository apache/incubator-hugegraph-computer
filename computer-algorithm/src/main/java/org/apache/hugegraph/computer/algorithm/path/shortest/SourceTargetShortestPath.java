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

package org.apache.hugegraph.computer.algorithm.path.shortest;

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class SourceTargetShortestPath implements Computation<ShortestPathMessage> {

    private static final Logger LOG = Log.logger(SourceTargetShortestPath.class);

    public static final String OPTION_SOURCE_ID = "source_target_shortest_path.source_id";
    public static final String OPTION_TARGET_ID = "source_target_shortest_path.target_id";
    public static final String OPTION_WEIGHT_PROPERTY =
            "source_target_shortest_path.weight_property";
    public static final String OPTION_DEFAULT_WEIGHT =
            "source_target_shortest_path.default_weight";

    /**
     * source vertex id
     */
    private String sourceId;

    /**
     * target vertex id
     */
    private String targetId;

    /**
     * weight property.
     * weight value must be a positive number.
     */
    private String weightProperty;

    /**
     * default weight.
     * default 1
     */
    private Double defaultWeight;

    @Override
    public String category() {
        return "path";
    }

    @Override
    public String name() {
        return "source_target_shortest_path";
    }

    @Override
    public void init(Config config) {
        this.sourceId = config.getString(OPTION_SOURCE_ID, "");
        if (StringUtils.isBlank(this.sourceId)) {
            throw new ComputerException("The param %s must not be blank, ", OPTION_SOURCE_ID);
        }

        this.targetId = config.getString(OPTION_TARGET_ID, "");
        if (StringUtils.isBlank(this.targetId)) {
            throw new ComputerException("The param %s must not be blank, ", OPTION_TARGET_ID);
        }

        this.weightProperty = config.getString(OPTION_WEIGHT_PROPERTY, "");

        this.defaultWeight = config.getDouble(OPTION_DEFAULT_WEIGHT, 1);
        if (this.defaultWeight <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                                        "actual got '%s'",
                                        OPTION_DEFAULT_WEIGHT, this.defaultWeight);
        }
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        ShortestPathValue value = new ShortestPathValue();
        vertex.value(value);

        // start from source vertex
        if (!this.idEquals(vertex, this.sourceId)) {
            vertex.inactivate();
            return;
        }

        value.updatePath(vertex, 0); // source vertex

        // source vertex == target vertex
        if (this.sourceId.equals(this.targetId)) {
            LOG.info("source vertex {} equals target vertex {}", this.sourceId, this.targetId);
            vertex.inactivate();
            return;
        }

        if (vertex.numEdges() <= 0) {
            // isolated vertex
            LOG.info("source vertex {} can not reach target vertex {}",
                     this.sourceId, this.targetId);
            vertex.inactivate();
            return;
        }

        vertex.edges().forEach(edge -> {
            ShortestPathMessage message = new ShortestPathMessage();
            message.addToPath(vertex, this.getEdgeWeight(edge));

            context.sendMessage(edge.targetId(), message);
        });

        vertex.inactivate();
    }

    // todo messages 似乎可以combine合并一下，只留下一条最短的距离message即可
    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<ShortestPathMessage> messages) {
        while (messages.hasNext()) {
            ShortestPathMessage message = messages.next();
            ShortestPathValue value = vertex.value();

            if (message.totalWeight() < value.totalWeight()) {
                // find a shorter path
                value.updatePath(message.path(), message.pathWeight(), vertex);
            }

            // reach target vertex or nowhere to go
            if (this.idEquals(vertex, this.targetId) || vertex.numEdges() <= 0) {
                continue;
            }

            vertex.edges().forEach(edge -> {
                ShortestPathMessage forwardMessage = new ShortestPathMessage();
                forwardMessage.addToPath(message.path(), message.pathWeight(),
                                         vertex, this.getEdgeWeight(edge));
                context.sendMessage(edge.targetId(), forwardMessage);
            });
        }

        vertex.inactivate();
    }

    /**
     * determine whether vertex.id and id are equal
     */
    private boolean idEquals(Vertex vertex, String id) {
        return vertex.id().value().toString().equals(id);
    }

    /**
     * get the weight of an edge by its weight property
     */
    private double getEdgeWeight(Edge edge) {
        double weight = this.defaultWeight;

        Value property = edge.property(this.weightProperty);
        if (property != null) {
            if (!property.isNumber()) {
                throw new ComputerException("The value of %s must be a numeric value, " +
                                            "actual got '%s'",
                                            this.weightProperty, property.string());
            }

            weight = ((DoubleValue) property).doubleValue();
            if (weight <= 0) {
                throw new ComputerException("The value of %s must be greater than 0, " +
                                            "actual got '%s'",
                                            this.weightProperty, property.string());
            }
        }
        return weight;
    }
}
