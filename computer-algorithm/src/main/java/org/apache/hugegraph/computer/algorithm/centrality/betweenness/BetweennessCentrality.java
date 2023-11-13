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

package org.apache.hugegraph.computer.algorithm.centrality.betweenness;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdSet;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class BetweennessCentrality implements Computation<BetweennessMessage> {

    private static final Logger LOG = Log.logger(BetweennessCentrality.class);

    public static final String OPTION_SAMPLE_RATE =
                               "betweenness_centrality.sample_rate";

    private double sampleRate;
    /*
     * Record the number of shortest paths for intermediate vertex, suppose
     * there are two shortest paths from A(source vertex) to E(current vertex),
     * [A -> B -> C -> E] and [A -> B -> D -> E]
     *
     * The saved data is as follows
     * A: B:2
     *    C:1
     *    D:1
     *    totalCount:2
     */
    private Map<Id, SeqCount> seqTable;

    @Override
    public String name() {
        return "betweenness_centrality";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void init(Config config) {
        this.sampleRate = config.getDouble(OPTION_SAMPLE_RATE, 1.0D);
        if (this.sampleRate <= 0.0D || this.sampleRate > 1.0D) {
            throw new ComputerException("The param %s must be in (0.0, 1.0], " +
                                        "actual got '%s'",
                                        OPTION_SAMPLE_RATE, this.sampleRate);
        }
        this.seqTable = new HashMap<>();
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        // First superstep is special, we just send vertex id to its neighbors
        BetweennessValue initialValue = new BetweennessValue(0.0D);
        initialValue.arrivedVertices().add(vertex.id());
        vertex.value(initialValue);
        if (vertex.numEdges() == 0) {
            return;
        }

        IdList sequence = new IdList();
        sequence.add(vertex.id());
        context.sendMessageToAllEdges(vertex, new BetweennessMessage(sequence));
        LOG.info("Finished compute-0 step");
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<BetweennessMessage> messages) {
        BetweennessValue value = vertex.value();
        // The betweenness value to be updated
        DoubleValue betweenness = value.betweenness();
        // Collect the vertices sent here this time
        IdSet arrivingVertices = new IdSet();
        while (messages.hasNext()) {
            BetweennessMessage message = messages.next();
            // The value contributed to the intermediate node on the path
            DoubleValue vote = message.vote();
            betweenness.value(betweenness.value() + vote.value());

            this.forward(context, vertex, message.sequence(), arrivingVertices);
        }
        value.arrivedVertices().addAll(arrivingVertices);

        boolean active = !this.seqTable.isEmpty();
        if (active) {
            this.sendMessage(context);
            this.seqTable.clear();
        } else {
            vertex.inactivate();
        }
    }

    private void forward(ComputationContext context, Vertex vertex,
                         IdList sequence, IdSet arrivingVertices) {
        if (sequence.size() == 0) {
            return;
        }

        BetweennessValue value = vertex.value();
        IdSet arrivedVertices = value.arrivedVertices();
        Id source = sequence.getFirst();
        // The source vertex is arriving at first time
        if (!arrivedVertices.contains(source)) {
            arrivingVertices.add(source);

            SeqCount seqCount = this.seqTable.computeIfAbsent(
                    source, k -> new SeqCount());
            seqCount.totalCount++;
            // Accumulate the number of shortest paths for intermediate vertices
            for (int i = 1; i < sequence.size(); i++) {
                Id id = sequence.get(i);
                Map<Id, Integer> idCounts = seqCount.idCount;
                idCounts.put(id, idCounts.getOrDefault(id, 0) + 1);
            }

            Id selfId = vertex.id();
            sequence.add(selfId);
            BetweennessMessage newMessage = new BetweennessMessage(sequence);
            for (Edge edge : vertex.edges()) {
                Id targetId = edge.targetId();
                if (this.sample(selfId, targetId, edge) &&
                    !sequence.contains(targetId)) {
                    context.sendMessage(targetId, newMessage);
                }
            }
        }
    }

    private void sendMessage(ComputationContext context) {
        for (SeqCount seqCount : this.seqTable.values()) {
            for (Map.Entry<Id, Integer> entry : seqCount.idCount.entrySet()) {
                double vote = (double) entry.getValue() / seqCount.totalCount;
                BetweennessMessage voteMessage = new BetweennessMessage(
                                                 new DoubleValue(vote));

                context.sendMessage(entry.getKey(), voteMessage);
            }
        }
    }

    private boolean sample(Id sourceId, Id targetId, Edge edge) {
        // Now just use the simplest way
        return Math.random() <= this.sampleRate;
    }

    private static class SeqCount {

        private final Map<Id, Integer> idCount;
        private int totalCount;

        public SeqCount() {
            this.idCount = new HashMap<>();
            this.totalCount = 0;
        }
    }
}
