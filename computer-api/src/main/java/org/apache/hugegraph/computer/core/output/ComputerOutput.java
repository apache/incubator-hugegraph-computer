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

package org.apache.hugegraph.computer.core.output;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;

/**
 * Computer output is used to output computer results. There is an output object
 * for every partition.
 */
public interface ComputerOutput {

    /**
     * Initialize the output. Create connection to target output system.
     */
    void init(Config config, int partition);

    /**
     * For each vertex in partition, this method is called regardless
     * vertex's status.
     */
    void write(Vertex vertex);

    /**
     * Write filter.
     * True to commit the computation result, otherwise not to commit.
     */
    default boolean filter(Config config, Computation computation, Vertex vertex) {
        return true;
    }

    /**
     * Merge output files of multiple partitions if applicable.
     */
    default void mergePartitions(Config config) {
        // pass
    }

    /**
     * Close the connection to target output system. Commit if target output
     * required.
     */
    void close();

    /**
     * The name of output property.
     */
    String name();
}
