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

package org.apache.hugegraph.computer.algorithm.path.rings.filter;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class FilterDescribe {

    private final List<DescribeItem> vertexFilter;
    private final List<DescribeItem> edgeFilter;

    @JsonCreator
    private FilterDescribe(@JsonProperty("vertex_filter")
                           List<DescribeItem> vertexFilter,
                           @JsonProperty("edge_filter")
                           List<DescribeItem> edgeFilter) {
        this.vertexFilter = CollectionUtils.isEmpty(vertexFilter) ?
                            ImmutableList.of() :
                            ImmutableList.copyOf(vertexFilter);
        this.edgeFilter = CollectionUtils.isEmpty(edgeFilter) ?
                          ImmutableList.of() :
                          ImmutableList.copyOf(edgeFilter);
    }

    public static FilterDescribe of(String describe) {
        return JsonUtil.fromJson(describe, FilterDescribe.class);
    }

    public List<DescribeItem> vertexFilter() {
        return this.vertexFilter;
    }

    public List<DescribeItem> edgeFilter() {
        return this.edgeFilter;
    }

    public static class DescribeItem {

        private final String label;
        private final String propertyFilter;

        @JsonCreator
        private DescribeItem(@JsonProperty(value = "label",
                                           required = true)
                             String label,
                             @JsonProperty(value = "property_filter",
                                           required = true)
                             String propertyFilter) {
            this.label = label;
            this.propertyFilter = propertyFilter;
        }

        public String label() {
            return this.label;
        }

        public String propertyFilter() {
            return this.propertyFilter;
        }
    }
}
