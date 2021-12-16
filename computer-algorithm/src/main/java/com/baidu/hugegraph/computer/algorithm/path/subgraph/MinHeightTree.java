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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.google.common.collect.ImmutableList;

public class MinHeightTree {

    private TreeNode root;
    private Integer treeHeight;
    private final Map<Integer, TreeNode> treeNodeIdMap;

    private List<List<Integer>> rootPaths;

    private MinHeightTree() {
        this.treeNodeIdMap = new HashMap<>();
        this.rootPaths = new ArrayList<>();
    }

    public static MinHeightTree build(QueryGraph graph) {
        // Build temporary MHT
        QueryGraph.Vertex temporaryRoot = graph.vertices().get(0);
        MinHeightTree temporaryTree = buildTreeWithRoot(graph, temporaryRoot);

        // Find real graph center node
        List<TreeNode> nodes = temporaryTree.nodes();
        while (nodes.size() > 2) {
            Iterator<TreeNode> itr = nodes.iterator();
            while (itr.hasNext()) {
                TreeNode node = itr.next();
                if (node.degree == 1) {
                    node.parent.degree -= 1;
                    for (TreeNode child : node.children) {
                        child.degree -= 1;
                    }
                    itr.remove();
                }
            }
        }
        TreeNode realRoot = nodes.get(0);

        // Build real MHT
        return buildTreeWithRoot(graph, realRoot.vertex);
    }

    private static MinHeightTree buildTreeWithRoot(QueryGraph graph,
                                                   QueryGraph.Vertex root) {
        int nodeId = 1;
        TreeNode treeRoot = new TreeNode(nodeId++, null, true,
                                         root, null);
        treeRoot.parent = treeRoot;

        MinHeightTree tree = new MinHeightTree();
        tree.root = treeRoot;

        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(treeRoot);
        while (!queue.isEmpty()) {
            TreeNode parent = queue.poll();
            QueryGraph.Vertex parentVertex = parent.vertex;

            ImmutableList<List<QueryGraph.Edge>> edges =
                                                 ImmutableList.of(
                                                 parentVertex.inEdges(),
                                                 parentVertex.outEdges());
            for (int i = 0; i < edges.size(); i++) {
                boolean isInEdge = (i & 1) == 0;
                for (QueryGraph.Edge edge : edges.get(i)) {
                    if (graph.isEdgeVisited(edge.id())) {
                        continue;
                    }
                    graph.visitEdge(edge.id());
                    String target;
                    if (isInEdge) {
                        target = edge.source();
                    } else {
                        target = edge.target();
                    }
                    QueryGraph.Vertex vertex = graph.findVertexById(target);
                    TreeNode node = new TreeNode(nodeId++, parent, isInEdge,
                                                 vertex, edge);
                    parent.addChild(node);
                    queue.offer(node);
                }
            }
        }

        afterBuild(tree);
        graph.resetEdgeVisited();

        return tree;
    }

    private static void afterBuild(MinHeightTree tree) {
        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(tree.root);
        while (!queue.isEmpty()) {
            TreeNode node = queue.poll();
            // If parent is root node
            int degree = 1;
            if (node.parent == node) {
                degree = 0;
            }
            node.degree = degree + node.children.size();

            tree.treeNodeIdMap.put(node.nodeId, node);

            for (TreeNode child : node.children) {
                queue.offer(child);
            }
        }

        tree.rootPaths = listPath(tree.root);

        String msg = "Can't find path in query graph";
        tree.treeHeight = tree.rootPaths
                              .stream()
                              .map(List::size)
                              .max(Integer::compareTo)
                              .orElseThrow(() -> {
                                    return new ComputerException(msg);
                              });
    }

    private static List<List<Integer>> listPath(TreeNode node) {
        List<List<Integer>> paths = new ArrayList<>();
        listPath(node, new ArrayList<>(), paths);
        paths.forEach(Collections::reverse);
        return paths;
    }

    private static void listPath(TreeNode node, List<Integer> path,
                                 List<List<Integer>> paths) {
        path.add(node.nodeId);
        if (CollectionUtils.isEmpty(node.children)) {
            paths.add(new ArrayList<>(path));
        } else {
            for (TreeNode child : node.children) {
                listPath(child, path, paths);
            }
        }
        path.remove(path.size() - 1);
    }

    public int treeHeight() {
        return this.treeHeight;
    }

    public TreeNode root() {
        return this.root;
    }

    public boolean matchRoot(Vertex vertex) {
        return this.root.match(vertex);
    }

    public TreeNode findNodeById(int nodeId) {
        return this.treeNodeIdMap.get(nodeId);
    }

    public List<List<Integer>> paths() {
        return this.rootPaths;
    }

    public Set<TreeNode> leaves() {
        Set<TreeNode> leaves = new HashSet<>();
        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(this.root);
        while (!queue.isEmpty()) {
            TreeNode node = queue.poll();
            if (CollectionUtils.isEmpty(node.children)) {
                leaves.add(node);
            } else {
                for (TreeNode child : node.children) {
                    queue.offer(child);
                }
            }
        }
        return leaves;
    }

    public boolean matchRootPath(List<Integer> needPath) {
        return this.rootPaths.stream().anyMatch(path -> {
            if (path.size() != needPath.size()) {
                return false;
            }
            boolean match = true;
            for (int i = 0; i < path.size(); i++) {
                if (!path.get(i).equals(needPath.get(i))) {
                    return false;
                }
            }
            return match;
        });
    }

    private List<TreeNode> nodes() {
        List<TreeNode> nodes = new LinkedList<>();
        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(this.root);
        while (!queue.isEmpty()) {
            TreeNode node = queue.poll();
            nodes.add(node);
            for (TreeNode child : node.children) {
                queue.offer(child);
            }
        }
        return nodes;
    }

    public static class TreeNode {

        private final int nodeId;
        private TreeNode parent;
        private final boolean inToParent;
        private final QueryGraph.Vertex vertex;
        private final QueryGraph.Edge edgeToParent;
        private final List<TreeNode> children;
        private int degree;

        public TreeNode(int nodeId, TreeNode parent, boolean inToParent,
                        QueryGraph.Vertex vertex,
                        QueryGraph.Edge edgeToParent) {
            this.nodeId = nodeId;
            this.parent = parent;
            this.inToParent = inToParent;
            this.vertex = vertex;
            this.edgeToParent = edgeToParent;
            this.children = new ArrayList<>();
        }

        public int nodeId() {
            return this.nodeId;
        }

        public TreeNode parent() {
            return this.parent;
        }

        public boolean isInToParent() {
            return this.inToParent;
        }

        public void addChild(TreeNode node) {
            this.children.add(node);
        }

        public boolean match(Vertex vertex) {
            return this.vertex.match(vertex);
        }

        public boolean match(Edge edge) {
            return this.edgeToParent.match(edge);
        }

        public QueryGraph.Vertex vertex() {
            return this.vertex;
        }
    }
}
