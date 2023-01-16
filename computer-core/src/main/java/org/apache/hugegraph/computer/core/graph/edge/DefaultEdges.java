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

package org.apache.hugegraph.computer.core.graph.edge;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hugegraph.computer.core.graph.GraphFactory;

public class DefaultEdges implements Edges {

    private final List<Edge> edges;

    public DefaultEdges(GraphFactory graphFactory, int capacity) {
        this.edges = graphFactory.createList(capacity);
    }

    @Override
    public int size() {
        return this.edges.size();
    }

    @Override
    public void add(Edge edge) {
        this.edges.add(edge);
    }

    @Override
    public Iterator<Edge> iterator() {
        return this.edges.iterator();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DefaultEdges)) {
            return false;
        }
        DefaultEdges other = (DefaultEdges) obj;
        return this.edges.equals(other.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.edges);
    }

    @Override
    public String toString() {
        return String.format("DefaultEdges{size=%s}", this.edges.size());
    }
}
