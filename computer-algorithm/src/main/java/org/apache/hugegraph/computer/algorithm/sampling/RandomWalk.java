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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class RandomWalk implements Computation<RandomWalkMessage> {

    private static final Logger LOG = Log.logger(RandomWalk.class);

    public static final String OPTION_WALK_PER_NODE = "random_walk.walk_per_node";
    public static final String OPTION_WALK_LENGTH = "random_walk.walk_length";

    public static final String OPTION_WEIGHT_PROPERTY = "random_walk.weight_property";
    public static final String OPTION_DEFAULT_WEIGHT = "random_walk.default_weight";
    public static final String OPTION_MIN_WEIGHT_THRESHOLD = "random_walk.min_weight_threshold";
    public static final String OPTION_MAX_WEIGHT_THRESHOLD = "random_walk.max_weight_threshold";

    public static final String OPTION_RETURN_FACTOR = "random_walk.return_factor";
    public static final String OPTION_INOUT_FACTOR = "random_walk.inout_factor";

    /**
     * Random
     */
    private Random random;

    /**
     * Number of times per vertex(source vertex) walks
     */
    private Integer walkPerNode;

    /**
     * Walk length
     */
    private Integer walkLength;

    /**
     * Weight property, related to the walking probability
     */
    private String weightProperty;

    /**
     * Biased walk
     * Default 1
     */
    private Double defaultWeight;

    /**
     * Weight less than this threshold will be truncated.
     * Default 0
     */
    private Double minWeightThreshold;

    /**
     * Weight greater than this threshold will be truncated.
     * Default Integer.MAX_VALUE
     */
    private Double maxWeightThreshold;

    /**
     * Controls the probability of re-walk to a previously walked vertex.
     * Default 1
     */
    private Double returnFactor;

    /**
     * Controls whether to walk inward or outward.
     * Default 1
     */
    private Double inOutFactor;

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
        this.random = new Random();

        this.walkPerNode = config.getInt(OPTION_WALK_PER_NODE, 3);
        if (this.walkPerNode <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                                        "actual got '%s'",
                                        OPTION_WALK_PER_NODE, this.walkPerNode);
        }

        this.walkLength = config.getInt(OPTION_WALK_LENGTH, 3);
        if (this.walkLength <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                                        "actual got '%s'",
                                        OPTION_WALK_LENGTH, this.walkLength);
        }

        this.weightProperty = config.getString(OPTION_WEIGHT_PROPERTY, "");

        this.defaultWeight = config.getDouble(OPTION_DEFAULT_WEIGHT, 1);
        if (this.defaultWeight <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                                        "actual got '%s'",
                                        OPTION_DEFAULT_WEIGHT, this.defaultWeight);
        }

        this.minWeightThreshold = config.getDouble(OPTION_MIN_WEIGHT_THRESHOLD, 0.0);
        if (this.minWeightThreshold < 0) {
            throw new ComputerException("The param %s must be greater than or equal 0, " +
                                        "actual got '%s'",
                                        OPTION_MIN_WEIGHT_THRESHOLD, this.minWeightThreshold);
        }

        this.maxWeightThreshold = config.getDouble(OPTION_MAX_WEIGHT_THRESHOLD, Double.MAX_VALUE);
        if (this.maxWeightThreshold < 0) {
            throw new ComputerException("The param %s must be greater than or equal 0, " +
                                        "actual got '%s'",
                                        OPTION_MAX_WEIGHT_THRESHOLD, this.maxWeightThreshold);
        }

        if (this.minWeightThreshold > this.maxWeightThreshold) {
            throw new ComputerException("%s must be greater than or equal %s, ",
                                        OPTION_MAX_WEIGHT_THRESHOLD, OPTION_MIN_WEIGHT_THRESHOLD);
        }

        this.returnFactor = config.getDouble(OPTION_RETURN_FACTOR, 1);
        if (this.returnFactor <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                                        "actual got '%s'",
                                        OPTION_RETURN_FACTOR, this.returnFactor);
        }

        this.inOutFactor = config.getDouble(OPTION_INOUT_FACTOR, 1);
        if (this.inOutFactor <= 0) {
            throw new ComputerException("The param %s must be greater than 0, " +
                                        "actual got '%s'",
                                        OPTION_INOUT_FACTOR, this.inOutFactor);
        }
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new IdListList());

        RandomWalkMessage message = new RandomWalkMessage();
        message.addToPath(vertex);

        if (vertex.numEdges() <= 0) {
            // isolated vertex
            this.savePath(vertex, message.path());
            vertex.inactivate();
            return;
        }

        vertex.edges().forEach(edge -> message.addToPreVertexAdjacence(edge.targetId()));

        for (int i = 0; i < walkPerNode; ++i) {
            // random select one edge and walk
            Edge selectedEdge = this.randomSelectEdge(null, null, vertex.edges());
            context.sendMessage(selectedEdge.targetId(), message);
        }

        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<RandomWalkMessage> messages) {
        while (messages.hasNext()) {
            RandomWalkMessage message = messages.next();
            // the last id of path is the previous id
            Id preVertexId = message.path().getLast();

            if (message.isFinish()) {
                this.savePath(vertex, message.path());

                vertex.inactivate();
                continue;
            }

            message.addToPath(vertex);

            if (vertex.numEdges() <= 0) {
                // there is nowhere to walk, finish eariler
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
                    this.savePath(vertex, message.path());
                } else {
                    context.sendMessage(sourceId, message);
                }

                vertex.inactivate();
                continue;
            }

            vertex.edges().forEach(edge -> message.addToPreVertexAdjacence(edge.targetId()));

            // random select one edge and walk
            Edge selectedEdge = this.randomSelectEdge(preVertexId, message.preVertexAdjacence(),
                                                      vertex.edges());
            context.sendMessage(selectedEdge.targetId(), message);
        }

        vertex.inactivate();
    }

    /**
     * random select one edge
     */
    private Edge randomSelectEdge(Id preVertexId, IdList preVertexAdjacenceIdList, Edges edges) {
        // TODO: use primitive array instead, like DoubleArray,
        //  in order to reduce memory fragmentation generated during calculations
        List<Double> weightList = new ArrayList<>();

        Iterator<Edge> iterator = edges.iterator();
        while (iterator.hasNext()) {
            Edge edge = iterator.next();
            // calculate edge weight
            double weight = this.getEdgeWeight(edge);
            double finalWeight = this.calculateEdgeWeight(preVertexId, preVertexAdjacenceIdList,
                                                          edge.targetId(), weight);
            // TODO: improve to avoid OOM
            weightList.add(finalWeight);
        }

        int selectedIndex = this.randomSelectIndex(weightList);
        Edge selectedEdge = this.selectEdge(edges.iterator(), selectedIndex);
        return selectedEdge;
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
        }

        // weight threshold truncation
        if (weight < this.minWeightThreshold) {
            weight = this.minWeightThreshold;
        }
        if (weight > this.maxWeightThreshold) {
            weight = this.maxWeightThreshold;
        }
        return weight;
    }

    /**
     * calculate edge weight
     */
    private double calculateEdgeWeight(Id preVertexId, IdList preVertexAdjacenceIdList,
                                       Id nextVertexId, double weight) {
        /*
         * 3 types of vertices.
         * 1. current vertex, called v
         * 2. previous vertex, called t
         * 3. current vertex outer vertex, called x(x1, x2.. xn)
         *
         * Definition of weight correction coefficient α:
         * if distance(t, x) = 0, then α = 1.0 / returnFactor
         * if distance(t, x) = 1, then α = 1.0
         * if distance(t, x) = 2, then α = 1.0 / inOutFactor
         *
         * Final edge weight π(v, x) = α * edgeWeight
         */
        double finalWeight = 0.0;
        if (preVertexId != null && preVertexId.equals(nextVertexId)) {
            // distance(t, x) = 0
            finalWeight = 1.0 / this.returnFactor * weight;
        } else if (preVertexAdjacenceIdList != null &&
                   preVertexAdjacenceIdList.contains(nextVertexId)) {
            // distance(t, x) = 1
            finalWeight = 1.0 * weight;
        } else {
            // distance(t, x) = 2
            finalWeight = 1.0 / this.inOutFactor * weight;
        }
        return finalWeight;
    }

    /**
     * random select index
     */
    private int randomSelectIndex(List<Double> weightList) {
        int selectedIndex = 0;
        double totalWeight = weightList.stream().mapToDouble(Double::doubleValue).sum();
        double randomNum = random.nextDouble() * totalWeight; // [0, totalWeight)

        // determine which interval the random number falls into
        double cumulativeWeight = 0;
        for (int i = 0; i < weightList.size(); ++i) {
            cumulativeWeight += weightList.get(i);
            if (randomNum < cumulativeWeight) {
                selectedIndex = i;
                break;
            }
        }
        return selectedIndex;
    }

    /**
     * select edge from iterator by index
     */
    private Edge selectEdge(Iterator<Edge> iterator, int selectedIndex) {
        Edge selectedEdge = null;

        int index = 0;
        while (iterator.hasNext()) {
            selectedEdge = iterator.next();
            if (index == selectedIndex) {
                break;
            }
            index++;
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
