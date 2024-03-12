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

package org.apache.hugegraph.computer.algorithm.path.shortest;

import org.apache.hugegraph.computer.core.combiner.IdSetMergeCombiner;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.master.MasterComputation;
import org.apache.hugegraph.computer.core.master.MasterComputationContext;
import org.apache.hugegraph.computer.core.master.MasterContext;

public class SingleSourceShortestPathMaster implements MasterComputation {

    public static final String SINGLE_SOURCE_SHORTEST_PATH_REACHED_TARGETS =
            "single_source_shortest_path.reached_targets";

    @Override
    public void init(MasterContext context) {
        context.registerAggregator(SINGLE_SOURCE_SHORTEST_PATH_REACHED_TARGETS,
                                   ValueType.ID_SET,
                                   IdSetMergeCombiner.class);
    }

    @Override
    public void close(MasterContext context) {
        // pass
    }

    @Override
    public boolean compute(MasterComputationContext context) {
        return true;
    }
}
