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

package com.baidu.hugegraph.computer.core.worker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.IdValueListList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.iterator.MapperIterator;

public class RingDetection<M extends IdValueList>
       implements One2OneComputation<M> {

    private static final IdValueListList EMPTY_VALUE = new IdValueListList();

//    @Override
//    public Iterator<M> computeMessages(WorkerContext context,
//                                       Vertex vertex,
//                                       Iterator<M> messages) {
//        List<M> possibleRings = new ArrayList<>();
//        while (messages.hasNext()) {
//            IdValueList sequence = messages.next();
//            IdValueList ring = this.possibleRing(vertex, sequence);
//            if (ring != null) {
//                possibleRings.add((M) ring);
//            }
//        }
//        return possibleRings.iterator();
//    }

    @Override
    public void sendMessage(WorkerContext context, Vertex vertex,
                            Iterator<M> results) {
        context.sendMessagesToAllEdgesIf(vertex, results, (result, target) -> {
            // only send to vertex while remaining ringId is
            // smallest, add equals 0 condition to make sure it can
            // go to first vertex again.
            IdValue ringId = result.get(0);
            return ringId.compareTo(target.idValue()) <= 0;
        });
        vertex.inactivate();
    }

    @Override
    public M initialValue(WorkerContext context, Vertex vertex) {
        vertex.value(EMPTY_VALUE);
        return (M) new IdValueList();
    }

    @Override
    public M computeMessage(WorkerContext context, Vertex vertex, M message) {
        return this.possibleRing(vertex, message);
    }

    private M possibleRing(Vertex vertex, M sequence) {
        IdValue selfId = vertex.id().idValue();

        if (sequence.size() > 0 && sequence.get(0).equals(selfId)) {
//            // seem unused
//            for (IdValue node : sequence.values()) {
//                if (selfId.compareTo(node) > 0) {
//                    findRing = false;
//                    break;
//                }
//            }
            IdValueList copy = sequence.copy();
            copy.add(selfId);
            IdValueListList value = vertex.value();
            value.add(copy);
            return null;
        } else {
            if (sequence.contains(selfId)) {
                return null;
            }
            IdValueList copy = sequence.copy();
            copy.add(selfId);
            return (M) copy;
        }
    }

    public static void main(String[] args) {
        RingDetection<IdValueList> ringDetection = new RingDetection<>();
    }
}
