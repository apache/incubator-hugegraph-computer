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

package org.apache.hugegraph.computer.core.master;

/**
 * The MasterContext is the interface for the algorithm's master-computation.
 * Algorithm's master-computation can get aggregators and get graph
 * information such as total vertex count and total edge count.
 */
public interface MasterComputationContext extends MasterContext {

    /**
     * @return the total vertex count of the graph. The value may vary from
     * superstep to superstep, because the algorithm may add or delete vertices
     * during superstep.
     */
    long totalVertexCount();

    /**
     * @return the total edge count of the graph. The value may vary from
     * superstep to superstep, because the algorithm may add or delete edges
     * during superstep.
     */
    long totalEdgeCount();

    /**
     * @return the vertex count that is inactive.
     */
    long finishedVertexCount();

    /**
     * @return the sent message count at current superstep.
     */
    long messageCount();

    /**
     * @return the message sent in bytes at current superstep.
     */
    long messageBytes();

    /**
     * @return the current superstep.
     */
    int superstep();
}
