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

package com.baidu.hugegraph.computer.algorithm.path.subgraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.util.Log;

public class SubGraphMatch implements Computation<SubGraphMatchMessage> {

    public static final String SUBGRAPH_OPTION = "subgraph.query_graph_config";

    private static final Logger LOG = Log.logger(SubGraphMatch.class.getName());
    private static final long PRINT_LOG_TIME = TimeUnit.SECONDS.toMillis(10L);

    private MinHeightTree subgraphTree;
    private Set<MinHeightTree.TreeNode> leaves;

    // Log
    private Vertex currentVertex;
    private long logTime;
    private long cartesianProductSize;
    private long currentCartesianProduct;

    @Override
    public String name() {
        return "subgraph_match";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        String subgraphConfig = config.getString(SUBGRAPH_OPTION, null);
        if (subgraphConfig == null) {
            throw new ComputerException("Config %s must not be null",
                                        SUBGRAPH_OPTION);
        }
        this.subgraphTree = MinHeightTree.build(new QueryGraph(subgraphConfig));
        this.leaves = this.subgraphTree.leaves();
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new SubGraphMatchValue());

        for (MinHeightTree.TreeNode leaf : this.leaves) {
            if (!leaf.match(vertex)) {
                continue;
            }
            SubGraphMatchMessage message = new SubGraphMatchMessage();
            message.merge(new MutablePair<>(leaf.nodeId(), vertex.id()));
            this.sendMessage(context, leaf, vertex, message);
        }

        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<SubGraphMatchMessage> messages) {
        if (context.superstep() >= this.subgraphTree.treeHeight()) {
            vertex.inactivate();
            return;
        }
        this.currentVertex = vertex;

        SubGraphMatchValue value = vertex.value();
        while (messages.hasNext()) {
            SubGraphMatchMessage message = messages.next();

            MinHeightTree.TreeNode lastNode =
                          this.subgraphTree.findNodeById(
                                            message.lastNode().getLeft());
            MinHeightTree.TreeNode parent = lastNode.parent();
            if (!parent.match(vertex)) {
                continue;
            }

            message.merge(new MutablePair<>(parent.nodeId(), vertex.id()));
            if (parent == this.subgraphTree.root()) {
                // Filter out invalid match path
                List<Pair<Integer, Id>> path = message.matchPath();
                Set<Id> ids = path.stream()
                                  .map(Pair::getRight)
                                  .collect(Collectors.toSet());
                if (ids.size() != path.size()) {
                    continue;
                }

                List<Integer> pathIds = path.stream()
                                            .map(Pair::getLeft)
                                            .collect(Collectors.toList());
                if (this.subgraphTree.matchRootPath(pathIds)) {
                    value.addMp(path);
                }
            } else {
                this.sendMessage(context, parent, vertex, message);
            }
        }

        if (context.superstep() == this.subgraphTree.treeHeight() - 1 &&
            this.subgraphTree.matchRoot(vertex)) {
            this.setValueRes(value);
        }

        vertex.inactivate();
    }

    private void sendMessage(ComputationContext context,
                             MinHeightTree.TreeNode node,
                             Vertex vertex, SubGraphMatchMessage message) {
        for (Edge edge : vertex.edges()) {
            if (node.isInToParent() == edge.isInverse() ||
                !node.match(edge)) {
                continue;
            }
            context.sendMessage(edge.targetId(), message);
        }
    }

    private void setValueRes(SubGraphMatchValue value) {
        List<List<Pair<Integer, Id>>> mp = value.mp();
        if (CollectionUtils.isEmpty(mp)) {
            return;
        }

        List<List<Integer>> paths = this.subgraphTree.paths();

        List<List<List<Pair<Integer, Id>>>> group = new ArrayList<>(
                                                        paths.size());
        for (int i = 0; i < paths.size(); i++) {
            group.add(new ArrayList<>());
        }
        for (List<Pair<Integer, Id>> mpItem : mp) {
            for (int i = 0; i < paths.size(); i++) {
                List<Integer> path = paths.get(i);
                if (this.pathMatch(path, mpItem)) {
                    group.get(i).add(mpItem);
                }
            }
        }
        value.clearMp();

        for (List<List<Pair<Integer, Id>>> groupItem : group) {
            if (groupItem.size() == 0) {
                return;
            }
        }

        // Cartesian Product
        this.logTime = System.currentTimeMillis();
        this.cartesianProductSize = 1L;
        this.currentCartesianProduct = 0L;
        for (List<List<Pair<Integer, Id>>> groupItem : group) {
            cartesianProductSize *= groupItem.size();
        }
        cartesianProductAndFilterRes(group, 0, new HashMap<>(), new HashSet<>(),
                                     value);
    }

    private boolean pathMatch(List<Integer> path,
                              List<Pair<Integer, Id>> mp) {
        if (path.size() != mp.size()) {
            return false;
        }
        List<Integer> mpPath = mp.stream()
                                 .map(Pair::getLeft)
                                 .collect(Collectors.toList());
        for (int i = 0; i < path.size(); i++) {
            if (!path.get(i).equals(mpPath.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void cartesianProductAndFilterRes(
            List<List<List<Pair<Integer, Id>>>> pathGroup, int groupIndex,
            Map<QueryGraph.Vertex, Id> resItem, Set<Id> resIds,
            SubGraphMatchValue value) {
        if (PRINT_LOG_TIME <= System.currentTimeMillis() - this.logTime) {
            LOG.info("Vertex {} is calculating cartesian, cartesian total " +
                     "size:{}, current cartesian size:{}",
                     this.currentVertex.id(), this.cartesianProductSize,
                     this.currentCartesianProduct);
            this.logTime = System.currentTimeMillis();
        }
        List<List<Pair<Integer, Id>>> group = pathGroup.get(groupIndex);
        for (List<Pair<Integer, Id>> pathMp : group) {
            List<QueryGraph.Vertex> notExistVertex = new ArrayList<>();
            List<Id> notExistId = new ArrayList<>();
            boolean needEnd = false;
            for (Pair<Integer, Id> mp : pathMp) {
                QueryGraph.Vertex vertex = this.subgraphTree.findNodeById(
                                                             mp.getLeft())
                                                            .vertex();
                if (!resItem.containsKey(vertex)) {
                    notExistVertex.add(vertex);
                    resItem.put(vertex, mp.getRight());
                    if (resIds.add(mp.getRight())) {
                        notExistId.add(mp.getRight());
                    }
                }

                if (!resItem.get(vertex).equals(mp.getRight()) ||
                    resItem.size() != resIds.size()) {
                    needEnd = true;
                }
            }

            if (!needEnd) {
                if (groupIndex == pathGroup.size() - 1) {
                    this.currentCartesianProduct++;
                    IdList realRes = new IdList();
                    realRes.addAll(resItem.values());
                    value.addRes(realRes);
                } else {
                    cartesianProductAndFilterRes(pathGroup, groupIndex + 1,
                                                 resItem, resIds, value);
                }
            }

            // Clear
            for (QueryGraph.Vertex vertex : notExistVertex) {
                resItem.remove(vertex);
            }
            for (Id id : notExistId) {
                resIds.remove(id);
            }
        }
    }
}
