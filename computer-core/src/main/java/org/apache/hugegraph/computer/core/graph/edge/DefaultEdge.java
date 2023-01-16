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

import java.util.Objects;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;

public class DefaultEdge implements Edge {

    private String label;
    private String name;
    private Id targetId;
    private Properties properties;

    public DefaultEdge(GraphFactory graphFactory) {
        this(graphFactory, Constants.EMPTY_STR, Constants.EMPTY_STR, null);
    }

    public DefaultEdge(GraphFactory graphFactory, String label,
                       String name, Id targetId) {
        this.label = label;
        this.name = name;
        this.targetId = targetId;
        this.properties = graphFactory.createProperties();
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
    public String name() {
        return this.name;
    }

    @Override
    public void name(String name) {
        this.name = name;
    }

    @Override
    public Id targetId() {
        return this.targetId;
    }

    @Override
    public void targetId(Id targetId) {
        this.targetId = targetId;
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DefaultEdge)) {
            return false;
        }
        DefaultEdge other = (DefaultEdge) obj;
        return Objects.equals(this.label, other.label) &&
               Objects.equals(this.targetId, other.targetId) &&
               Objects.equals(this.name, other.name) &&
               this.properties.equals(other.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.label, this.name, this.targetId,
                            this.properties);
    }

    @Override
    public String toString() {
        return String.format("DefaultEdge{label=%s, name=%s, targetId=%s}",
                             this.label, this.name, this.targetId);
    }
}
