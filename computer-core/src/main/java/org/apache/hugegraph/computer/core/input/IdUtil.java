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

package org.apache.hugegraph.computer.core.input;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.SplicingIdGenerator;

import com.google.common.collect.ImmutableList;

public class IdUtil {

    public static final String NUMBER_ID_PREFIX = "L";
    public static final String STRING_ID_PREFIX = "S";

    private static String writeString(Object rawId) {
        Id id = HugeConverter.convertId(rawId);
        String idString = id.toString();
        return (id.idType() == IdType.LONG ?
                NUMBER_ID_PREFIX : STRING_ID_PREFIX).concat(idString);
    }

    private static List<Object> sortValues(Edge edge, EdgeLabel edgeLabel) {
        List<String> sortKeys = edgeLabel.sortKeys();
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }
        List<Object> propValues = new ArrayList<>(sortKeys.size());
        for (String sk : sortKeys) {
            Object property = edge.property(sk);
            E.checkState(property != null,
                         "The value of sort key '%s' can't be null", sk);
            propValues.add(property);
        }
        return propValues;
    }

    public static String assignEdgeId(Edge edge, EdgeLabel edgeLabel) {
        return SplicingIdGenerator.concat(
               writeString(edge.sourceId()),
               String.valueOf(edgeLabel.id()),
               SplicingIdGenerator.concatValues(sortValues(edge, edgeLabel)),
               writeString(edge.targetId()));
    }
}
