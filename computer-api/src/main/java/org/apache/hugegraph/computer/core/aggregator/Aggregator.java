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

package org.apache.hugegraph.computer.core.aggregator;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.value.Value;

public interface Aggregator<V extends Value> {

    /**
     * Used by worker to aggregate a new value when compute a vertex.
     * The combination method needs to be commutative and associative.
     * Can be called in worker computation compute() or afterSuperstep().
     * @param value The value to be aggregated
     */
    void aggregateValue(V value);

    /**
     * Used by worker to aggregate an int value. For performance reasons, it
     * can aggregate without create an IntValue object.
     */
    default void aggregateValue(int value) {
        throw new ComputerException("Not implemented: aggregateValue(int)");
    }

    /**
     * Used by worker to aggregate a long value. For performance reasons, it
     * can aggregate without create a LongValue object.
     */
    default void aggregateValue(long value) {
        throw new ComputerException("Not implemented: aggregateValue(long)");
    }

    /**
     * Used by worker to aggregate a float value. For performance reasons, it
     * can aggregate without create a FloatValue object.
     */
    default void aggregateValue(float value) {
        throw new ComputerException("Not implemented: aggregateValue(float)");
    }

    /**
     * Used by worker to aggregate a double value. For performance reasons,
     * it can aggregate without create a DoubleValue object.
     */
    default void aggregateValue(double value) {
        throw new ComputerException("Not implemented: aggregateValue(double)");
    }

    /**
     * Used by worker or master to get the aggregated value. The worker
     * get an aggregated value of previous superstep in current superstep.
     * The master can get an aggregated value of current superstep when
     * master-computation compute().
     */
    V aggregatedValue();

    /**
     * Used by worker or master to set current aggregated value directly. The
     * worker set aggregated value and then send to master for further
     * aggregation. The master set aggregated value and then used by workers in
     * the next superstep.
     */
    void aggregatedValue(V value);

    /**
     * Return cloned object of this instance.
     */
    Aggregator<V> copy();

    /**
     * Repair the object because some fields may not be deserialized.
     */
    void repair(ComputerContext context);
}
