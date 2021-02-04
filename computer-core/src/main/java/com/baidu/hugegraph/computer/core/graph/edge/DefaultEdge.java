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

package com.baidu.hugegraph.computer.core.graph.edge;

import java.util.Objects;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;

public class DefaultEdge implements Edge {

    private Id targetId;
    private Value value;
    private Properties properties;

    public DefaultEdge() {
        this(null, null);
    }

    public DefaultEdge(Id targetId, Value value) {
        this.targetId = targetId;
        this.value = value;
        this.properties = new DefaultProperties();
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
    @SuppressWarnings("unchecked")
    public <V extends Value> V value() {
        return (V) this.value;
    }

    @Override
    public <V extends Value> void value(V value) {
        this.value = value;
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DefaultEdge)) {
            return false;
        }
        DefaultEdge other = (DefaultEdge) obj;
        return this.targetId.equals(other.targetId) &&
               this.value.equals(other.value) &&
               this.properties.equals(other.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.targetId, this.value, this.properties);
    }

    @Override
    public String toString() {
        return String.format("DefaultEdge{targetId=%s, value=%s}",
                             this.targetId, this.value);
    }
}
