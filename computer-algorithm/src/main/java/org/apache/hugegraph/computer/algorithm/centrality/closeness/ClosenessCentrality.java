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

package org.apache.hugegraph.computer.algorithm.centrality.closeness;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.NumericUtil;
import org.slf4j.Logger;

public class ClosenessCentrality implements Computation<ClosenessMessage> {

    private static final Logger LOG = Log.logger(ClosenessCentrality.class);

    public static final String OPTION_WEIGHT_PROPERTY =
                               "closeness_centrality.weight_property";
    public static final String OPTION_SAMPLE_RATE =
                               "closeness_centrality.sample_rate";

    private String weightProp;
    private double sampleRate;

    @Override
    public String name() {
        return "closeness_centrality";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void init(Config config) {
        this.weightProp = config.getString(OPTION_WEIGHT_PROPERTY, "");
        this.sampleRate = config.getDouble(OPTION_SAMPLE_RATE, 1.0D);
        if (this.sampleRate <= 0.0D || this.sampleRate > 1.0D) {
            throw new ComputerException("The param %s must be in (0.0, 1.0], " +
                                        "actual got '%s'",
                                        OPTION_SAMPLE_RATE, this.sampleRate);
        }
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        // Set empty map as initial value
        vertex.value(new ClosenessValue());

        // Send messages to adjacent edges
        for (Edge edge : vertex.edges()) {
            Id senderId = vertex.id();
            // Get property value
            double value = this.weightValue(edge.property(this.weightProp));
            DoubleValue distance = new DoubleValue(value);
            ClosenessMessage message = new ClosenessMessage(senderId, senderId,
                                                            distance);
            context.sendMessage(edge.targetId(), message);
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<ClosenessMessage> messages) {
        Id selfId = vertex.id();
        // Save the distance from other vertices to self
        ClosenessValue localValue = vertex.value();
        boolean active = false;
        while (messages.hasNext()) {
            active = true;
            ClosenessMessage message = messages.next();
            Id senderId = message.senderId();
            // In theory, it won't happen, defensive programming
            if (selfId.equals(senderId)) {
                continue;
            }
            Id startId = message.startId();
            if (selfId.equals(startId)) {
                continue;
            }

            DoubleValue oldValue = localValue.get(startId);
            DoubleValue newValue = message.distance();
            // If the id already exists and the new value >= old value, skip it
            if (oldValue != null && newValue.compareTo(oldValue) >= 0) {
                continue;
            }
            // Update local saved values (take smaller value)
            localValue.put(startId, newValue);
            // Send this smaller value to neighbors
            this.sendMessage(context, vertex, senderId, startId, newValue);
        }
        if (!active) {
            vertex.inactivate();
        }
    }

    private void sendMessage(ComputationContext context, Vertex vertex,
                             Id senderId, Id startId, DoubleValue newValue) {
        Id selfId = vertex.id();
        double baseNewValue = this.weightValue(newValue);
        for (Edge edge : vertex.edges()) {
            Id targetId = edge.targetId();
            if (senderId.equals(targetId) || startId.equals(targetId)) {
                continue;
            }
            if (!sample(selfId, targetId, edge)) {
                continue;
            }
            // Update distance information
            double updatedValue = baseNewValue + this.weightValue(edge.property(
                                                 this.weightProp));
            DoubleValue newDistance = new DoubleValue(updatedValue);
            ClosenessMessage message = new ClosenessMessage(selfId, startId,
                                                            newDistance);
            context.sendMessage(targetId, message);
        }
    }

    private boolean sample(Id sourceId, Id targetId, Edge edge) {
        // Now just use the simplest way
        return Math.random() <= this.sampleRate;
    }

    private double weightValue(Value rawValue) {
        if (rawValue == null) {
            return 1.0D;
        }
        if (rawValue.isNumber()) {
            return NumericUtil.convertToNumber(rawValue).doubleValue();
        } else {
            throw new ComputerException("The weight property can only be " +
                                        "either Long or Int or Double or " +
                                        "Float, but got %s",
                                        rawValue.valueType());
        }
    }
}
