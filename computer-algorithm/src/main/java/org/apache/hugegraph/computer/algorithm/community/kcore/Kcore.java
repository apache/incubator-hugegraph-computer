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

package org.apache.hugegraph.computer.algorithm.community.kcore;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

import com.google.common.collect.Iterators;

public class Kcore implements Computation<Id> {

    public static final String OPTION_K = "kcore.k";
    public static final int K_DEFAULT_VALUE = 3;

    private final KcoreValue initValue = new KcoreValue();

    private int k = 0;

    @Override
    public String name() {
        return "kcore";
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void init(Config config) {
        this.k = config.getInt(OPTION_K, K_DEFAULT_VALUE);
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        KcoreValue value = this.initValue;
        value.core((Id) vertex.id().copy());
        vertex.value(value);

        if (vertex.numEdges() < this.k) {
            value.degree(0);
            /*
             * TODO: send int type message at phase 1, it's different from id
             * type of phase 2 (wcc message), need support switch message type.
             */
            context.sendMessageToAllEdges(vertex, vertex.id());
            vertex.inactivate();
        } else {
            value.degree(vertex.numEdges());
            assert vertex.active();
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<Id> messages) {
        KcoreValue value = vertex.value();
        if (!value.active()) {
            // Ignore messages of deleted vertex
            vertex.inactivate();
            return;
        }

        int superstep = context.superstep();
        if (superstep <= 2) {
            int deleted = Iterators.size(messages);
            assert value.active();
            if (value.decreaseDegree(deleted) < this.k) {
                // From active to inactive, delete self vertex
                value.degree(0);
                if (superstep == 1) {
                    context.sendMessageToAllEdges(vertex, vertex.id());
                }
                vertex.inactivate();
            } else {
                // From active to active, do wcc from superstep 2
                if (superstep == 2) {
                    // Start wcc
                    context.sendMessageToAllEdgesIf(vertex, vertex.id(),
                                                    (source, target) -> {
                        return source.compareTo(target) < 0;
                    });
                    vertex.inactivate();
                } else {
                    // Keep active at superstep 1 to continue superstep 2
                    assert superstep == 1;
                    assert vertex.active();
                }
            }
        } else {
            // Do wcc
            assert superstep > 2;
            Id message = Combiner.combineAll(context.combiner(), messages);
            if (value.core().compareTo(message) > 0) {
                value.core(message);
                context.sendMessageToAllEdges(vertex, message);
            }
            vertex.inactivate();
        }
    }
}
