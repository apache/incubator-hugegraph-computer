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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

public class QueryGraphDescribe {

    public static List<VertexDescribe> of(String describe) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(
                                describe,
                                new TypeReference<List<VertexDescribe>>() { });
        } catch (JsonProcessingException e) {
            throw new ComputerException("Failed to parsing graph describe " +
                                        "from config: %s", e, describe);
        }
    }

    public static class VertexDescribe {

        private final String id;
        private final String label;
        private final Expression propertyFilter;
        private final List<EdgeDescribe> edges;

        @JsonCreator
        public VertexDescribe(@JsonProperty(value = "id", required = true)
                              String id,
                              @JsonProperty(value = "label", required = true)
                              String label,
                              @JsonProperty(value = "property_filter")
                              String propertyFilter,
                              @JsonProperty(value = "edges")
                              List<EdgeDescribe> edges) {
            this.id = id;
            this.label = label;
            if (StringUtils.isNotEmpty(propertyFilter)) {
                this.propertyFilter = AviatorEvaluator.compile(propertyFilter);
            } else {
                this.propertyFilter = null;
            }
            this.edges = CollectionUtils.isNotEmpty(edges) ?
                         edges :
                         new ArrayList<>();
        }

        public String id() {
            return this.id;
        }

        public String label() {
            return this.label;
        }

        public Expression propertyFilter() {
            return this.propertyFilter;
        }

        public List<EdgeDescribe> edges() {
            return this.edges;
        }
    }

    public static class EdgeDescribe {

        private final String targetId;
        private final String label;
        private final Expression propertyFilter;

        @JsonCreator
        public EdgeDescribe(@JsonProperty(value = "targetId", required = true)
                            String targetId,
                            @JsonProperty(value = "label", required = true)
                            String label,
                            @JsonProperty(value = "property_filter")
                            String propertyFilter) {
            this.targetId = targetId;
            this.label = label;
            if (StringUtils.isNotEmpty(propertyFilter)) {
                this.propertyFilter = AviatorEvaluator.compile(propertyFilter);
            } else {
                this.propertyFilter = null;
            }
        }

        public String targetId() {
            return this.targetId;
        }

        public String label() {
            return this.label;
        }

        public Expression propertyFilter() {
            return this.propertyFilter;
        }
    }
}
