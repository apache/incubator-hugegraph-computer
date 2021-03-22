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

package com.baidu.hugegraph.computer.core.aggregator;

import com.baidu.hugegraph.computer.core.graph.value.Value;

public interface Aggregator4Worker {

    /**
     * Set aggregate value after superstep. The aggregator will be sent to
     * master when current superstep finish.
     * @param value The value to be aggregated
     */
    <V extends Value> void aggregateValue(String name, V value);

    /**
     * Get the aggregated value. The aggregated value is sent by master
     * before current superstep start.
     */
    <V extends Value> V aggregatedValue(String name);
}
