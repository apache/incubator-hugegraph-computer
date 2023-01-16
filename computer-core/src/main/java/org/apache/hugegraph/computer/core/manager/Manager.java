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

package org.apache.hugegraph.computer.core.manager;

import org.apache.hugegraph.computer.core.config.Config;

/**
 * Manager is used to manage a specified resource, like AggregatorManager and
 * CommunicationManager. A manager is created and initialized
 * by {@link #init(Config)}} before all supersteps start, and destroyed by
 * {@link #close} after all supersteps ended.
 */
public interface Manager {

    /**
     * The unique identify name.
     */
    String name();

    /**
     * Used to add the resources needed by the computation.
     * Be called only one time before all supersteps start.
     */
    default void init(Config config) {
        // pass
    }

    /**
     * Used to notify all managers that master or worker is inited.
     * Be called only one time before all supersteps start.
     */
    default void inited(Config config) {
        // pass
    }

    /**
     * Close the resources used in the computation.
     * Be called only one time after all supersteps ended.
     */
    default void close(Config config) {
        // pass
    }

    /**
     * Do some initialization for a superstep.
     * Be called before a superstep. Subclass should override this method
     * if wants to do something before a superstep.
     */
    default void beforeSuperstep(Config config, int superstep) {
        // pass
    }

    /**
     * Do some clean up for a superstep.
     * Be called after a superstep. Subclass should override this method
     * if wants to do something after a superstep.
     */
    default void afterSuperstep(Config config, int superstep) {
        // pass
    }
}
