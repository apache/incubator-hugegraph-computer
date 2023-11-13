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

package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Random;

public class RandomWalk implements Computation<RandomWalkMessage> {

    private static final Logger LOG = Log.logger(RandomWalk.class);

    public static final String OPTION_WALK_PER_NODE = "randomwalk.walk_per_node";
    public static final String OPTION_WALK_LENGTH = "randomwalk.walk_length";

    /**
     * number of times per vertex(source vertex) walks
     */
    private Integer walkPerNode;

    /**
     * walk length
     */
    private Integer walkLength;

    /**
     * random
     */
    private Random random;

    @Override
    public String category() {
        return "sampling";
    }

    @Override
    public String name() {
        return "random_walk";
    }

    @Override
    public void init(Config config) {
        this.walkPerNode = config.getInt(OPTION_WALK_PER_NODE, 3);
        if (this.walkPerNode <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                    "actual got '%s'",
                    OPTION_WALK_PER_NODE, this.walkPerNode);
        }
        LOG.info("[RandomWalk] algorithm param, {}: {}", OPTION_WALK_PER_NODE, walkPerNode);

        this.walkLength = config.getInt(OPTION_WALK_LENGTH, 3);
        if (this.walkLength <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                    "actual got '%s'",
                    OPTION_WALK_LENGTH, this.walkLength);
        }
        LOG.info("[RandomWalk] algorithm param, {}: {}", OPTION_WALK_LENGTH, walkLength);

        this.random = new Random();
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new IdListList());

        RandomWalkMessage message = new RandomWalkMessage();
        message.addToPath(vertex);

        if (vertex.numEdges() <= 0) {
            // isolated vertex
            this.savePath(vertex, message.path()); // save result
            vertex.inactivate();
            return;
        }

        for (int i = 0; i < walkPerNode; ++i) {
            // random select one edge and walk
            Edge selectedEdge = this.randomSelectEdge(vertex.edges());
            context.sendMessage(selectedEdge.targetId(), message);
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<RandomWalkMessage> messages) {
        while (messages.hasNext()) {
            RandomWalkMessage message = messages.next();

            if (message.isFinish()) {
                this.savePath(vertex, message.path()); // save result

                vertex.inactivate();
                continue;
            }

            message.addToPath(vertex);

            if (vertex.numEdges() <= 0) {
                // there is nowhere to walk，finish eariler
                message.finish();
                context.sendMessage(this.getSourceId(message.path()), message);

                vertex.inactivate();
                continue;
            }

            if (message.path().size() >= this.walkLength + 1) {
                message.finish();
                Id sourceId = this.getSourceId(message.path());

                if (vertex.id().equals(sourceId)) {
                    // current vertex is the source vertex，no need to send message once more
                    this.savePath(vertex, message.path()); // save result
                } else {
                    context.sendMessage(sourceId, message);
                }

                vertex.inactivate();
                continue;
            }

            // random select one edge and walk
            Edge selectedEdge = this.randomSelectEdge(vertex.edges());
            context.sendMessage(selectedEdge.targetId(), message);
        }
    }

    /**
     * random select one edge
     */
    private Edge randomSelectEdge(Edges edges) {
        Edge selectedEdge = null;
        int randomNum = random.nextInt(edges.size());

        int i = 0;
        Iterator<Edge> iterator = edges.iterator();
        while (iterator.hasNext()) {
            selectedEdge = iterator.next();
            if (i == randomNum) {
                break;
            }
            i++;
        }

        return selectedEdge;
    }

    /**
     * get source id of path
     */
    private Id getSourceId(IdList path) {
        // the first id of path is the source id
        return path.getFirst();
    }

    /**
     * save path
     */
    private void savePath(Vertex sourceVertex, IdList path) {
        IdListList curValue = sourceVertex.value();
        curValue.add(path.copy());
    }
}
