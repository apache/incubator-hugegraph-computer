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

package org.apache.hugegraph.computer.algorithm.centrality.closeness;

import java.util.Map;

import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.MapValue;

public class ClosenessValue extends MapValue<DoubleValue> {

    @Override
    public Double value() {
        // Cumulative distance
        double centrality = 0;
        for (Map.Entry<Id, DoubleValue> entry : this.entrySet()) {
            centrality += 1.0D / entry.getValue().value();
        }
        return centrality;
    }

    @Override
    public String string() {
        return this.value().toString();
    }
}
