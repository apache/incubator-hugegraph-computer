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

package org.apache.hugegraph.computer.core.graph.vertex;

import java.util.Objects;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;

public class DefaultVertex implements Vertex {

    private String label;
    private Id id;
    private Value value;
    private Edges edges;
    private Properties properties;
    private boolean active;

    public DefaultVertex(GraphFactory graphFactory) {
        this(graphFactory, null, null);
    }

    public DefaultVertex(GraphFactory graphFactory, Id id, Value value) {
        this(graphFactory, Constants.EMPTY_STR, id, value);
    }

    public DefaultVertex(GraphFactory graphFactory, String label, Id id,
                         Value value) {
        this.label = label;
        this.id = id;
        this.value = value;
        this.edges = graphFactory.createEdges();
        this.properties = graphFactory.createProperties();
        this.active = true;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public void label(String label) {
        this.label = label;
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public void id(Id id) {
        this.id = id;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value> V value() {
        return (V) this.value;
    }

    @Override
    public <V extends Value> void value(V value) {
        this.value = value;
    }

    @Override
    public int numEdges() {
        return this.edges.size();
    }

    @Override
    public Edges edges() {
        return this.edges;
    }

    @Override
    public void edges(Edges edges) {
        this.edges = edges;
    }

    @Override
    public void addEdge(Edge edge) {
        this.edges.add(edge);
    }

    @Override
    public Properties properties() {
        return this.properties;
    }

    @Override
    public void properties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public <T extends Value> T property(String key) {
        return this.properties.get(key);
    }

    @Override
    public boolean active() {
        return this.active;
    }

    @Override
    public void inactivate() {
        this.active = false;
    }

    @Override
    public void reactivate() {
        this.active = true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DefaultVertex)) {
            return false;
        }
        DefaultVertex other = (DefaultVertex) obj;
        return this.active == other.active &&
               Objects.equals(this.id, other.id) &&
               Objects.equals(this.value, other.value) &&
               Objects.equals(this.edges, other.edges) &&
               Objects.equals(this.properties, other.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.value, this.edges,
                            this.properties, this.active);
    }

    @Override
    public String toString() {
        return String.format("DefaultVertex{id=%s, value=%s, edges.size=%s, " +
                             "active=%s}", this.id, this.value,
                             this.edges.size(), this.active);
    }
}
