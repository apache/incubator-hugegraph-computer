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
 * Master-computation is computation that can determine whether to continue
 * next superstep. It runs on master. It can perform centralized computation
 * between supersteps. It runs after every superstep.
 *
 * The communication between the master and workers is performed via
 * aggregators. The value of aggregators are collected by the master before
 * {@link #compute(MasterComputationContext)} ()} is called. This means
 * aggregator values used by the workers are consistent with aggregator
 * values from the master in the same superstep.
 */
public interface MasterComputation {

    /**
     * Initialize the master-computation. Register algorithms's aggregators in
     * this method. Create resources used in compute. Be called before all
     * supersteps start.
     */
    void init(MasterContext context);

    /**
     * Close the resource created in {@link #init(MasterContext)}.
     * Be called after all supersteps.
     */
    void close(MasterContext context);

    /**
     * The master-algorithm can use aggregators to determine whether to
     * continue the next execution or not. Be called at the end of
     * a superstep.
     * @return true if want to continue the next iteration.
     */
    boolean compute(MasterComputationContext context);
}
