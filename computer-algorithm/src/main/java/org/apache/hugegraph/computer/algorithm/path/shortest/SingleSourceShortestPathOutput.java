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

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.output.hg.HugeGraphOutput;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.util.JsonUtil;

public class SingleSourceShortestPathOutput extends HugeGraphOutput<String> {

    @Override
    protected void prepareSchema() {
        this.client().schema().propertyKey(this.name())
            .asText()
            .writeType(this.writeType())
            .valueList()
            .ifNotExist()
            .create();
    }

    @Override
    protected String value(Vertex vertex) {
        SingleSourceShortestPathValue value = vertex.value();

        Map map = new HashMap();
        map.put("path", value.path().toString());
        map.put("total_weight", value.totalWeight());
        return JsonUtil.toJson(map);
    }

    @Override
    public boolean filter(Config config, Computation computation, Vertex vertex) {
        SingleSourceShortestPath sssp = (SingleSourceShortestPath) computation;
        return sssp.getTargetIdSet() == null || sssp.getTargetIdSet().contains(vertex.id());
    }
}
