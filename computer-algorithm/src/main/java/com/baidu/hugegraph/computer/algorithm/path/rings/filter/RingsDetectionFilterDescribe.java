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

package com.baidu.hugegraph.computer.algorithm.path.rings.filter;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.algorithm.path.filter.FilterDescribe;
import com.baidu.hugegraph.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class RingsDetectionFilterDescribe {

    private final List<FilterDescribe> vertexFilter;
    private final List<FilterDescribe> edgeFilter;

    @JsonCreator
    private RingsDetectionFilterDescribe(@JsonProperty("vertex_filter")
                                         List<FilterDescribe> vertexFilter,
                                         @JsonProperty("edge_filter")
                                         List<FilterDescribe> edgeFilter) {
        this.vertexFilter = CollectionUtils.isEmpty(vertexFilter) ?
                            ImmutableList.of() :
                            ImmutableList.copyOf(vertexFilter);
        this.edgeFilter = CollectionUtils.isEmpty(edgeFilter) ?
                          ImmutableList.of() :
                          ImmutableList.copyOf(edgeFilter);
    }

    public static RingsDetectionFilterDescribe of(String describe) {
        return JsonUtil.fromJson(describe, RingsDetectionFilterDescribe.class);
    }

    public List<FilterDescribe> vertexFilter() {
        return this.vertexFilter;
    }

    public List<FilterDescribe> edgeFilter() {
        return this.edgeFilter;
    }
}
