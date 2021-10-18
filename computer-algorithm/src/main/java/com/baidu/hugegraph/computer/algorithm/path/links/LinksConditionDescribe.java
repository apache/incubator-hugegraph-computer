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

package com.baidu.hugegraph.computer.algorithm.path.links;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.algorithm.path.filter.PropertyFilterDescribe;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class LinksConditionDescribe {

    private final ImmutableList<String> startVertexes;
    private final PropertyFilterDescribe vertexEndCondition;
    private final PropertyFilterDescribe edgeEndCondition;
    private final PropertyFilterDescribe edgeCompareCondition;

    @JsonCreator
    private LinksConditionDescribe(
            @JsonProperty(value = "start_vertexes", required = true)
            List<String> startVertexes,
            @JsonProperty(value = "vertex_end_condition")
            PropertyFilterDescribe vertexEndCondition,
            @JsonProperty(value = "edge_end_condition")
            PropertyFilterDescribe edgeEndCondition,
            @JsonProperty(value = "edge_compare_condition", required = true)
            PropertyFilterDescribe edgeCompareCondition) {
        E.checkArgument(CollectionUtils.isNotEmpty(startVertexes),
                        "Parameter start_vertexes must not be empty");
        this.startVertexes = ImmutableList.copyOf(startVertexes);
        E.checkArgument(vertexEndCondition != null || edgeEndCondition != null,
                        "Parameter vertex_end_condition and " +
                        "edge_end_condition can't all be null");
        this.vertexEndCondition = vertexEndCondition;
        this.edgeEndCondition = edgeEndCondition;
        this.edgeCompareCondition = edgeCompareCondition;
    }

    public static LinksConditionDescribe of(String describe) {
        return JsonUtil.fromJson(describe, LinksConditionDescribe.class);
    }

    public List<String> startVertexes() {
        return this.startVertexes;
    }

    public PropertyFilterDescribe vertexEndCondition() {
        return this.vertexEndCondition;
    }

    public PropertyFilterDescribe edgeEndCondition() {
        return this.edgeEndCondition;
    }

    public PropertyFilterDescribe edgeCompareCondition() {
        return this.edgeCompareCondition;
    }
}
