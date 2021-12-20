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

package com.baidu.hugegraph.computer.algorithm.path.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PropertyFilterDescribe {

    private final String label;
    private final String propertyFilter;

    @JsonCreator
    public PropertyFilterDescribe(@JsonProperty(value = "label",
                                                required = true)
                                  String label,
                                  @JsonProperty(value = "property_filter")
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
