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

package com.baidu.hugegraph.computer.core.master;

/**
 * Master computation runs on master. It can perform centralized computation
 * between supersteps. It will run every superstep before workers.
 *
 * The communication with the workers is performed via aggregators. The value
 * of aggregators are collected by the master before
 * {@link #compute(MasterContext)} ()} is called. This means aggregator
 * values used by the workers are consistent with aggregator values from the
 * master in the same superstep and aggregator used by the master are
 * consistent with aggregator values from the workers from the previous
 * superstep.
 */
public interface MasterComputation {

    /**
     * Initialize the master computation. Register algorithms's aggregators in
     * this method. Create resources used in compute.
     */
    void init(MasterContext context);

    /**
     * This method is called before a superstep. In this method, the
     * algorithm can using aggregators to determine whether to continue
     * execution.
     * @return true if continue execution, false if not.
     */
    boolean compute(MasterContext context);

    /**
     * Close the resource created in {@link #init(MasterContext)}. This
     * method is executed after all iteration.
     */
    void close();
}
