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

import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.util.E;

public class DefaultAggregator<V extends Value> implements Aggregator<V> {

    private final ValueType type;
    private final Class<? extends Combiner<V>> combinerClass;

    private transient Combiner<V> combiner;
    private transient ThreadLocal<V> localValue;

    private V value;

    public DefaultAggregator(ComputerContext context, ValueType type,
                             Class<? extends Combiner<V>> combinerClass,
                             V defaultValue) {
        E.checkArgument(type != null,
                        "The value type of aggregator can't be null");
        E.checkArgument(combinerClass != null,
                        "The combiner of aggregator can't be null");
        this.type = type;
        this.combinerClass = combinerClass;

        if (defaultValue != null) {
            this.checkValue(defaultValue);
        }
        this.value = defaultValue;

        if (context != null) {
            this.repair(context);
        }

        E.checkArgument(this.value != null,
                        "Must provide default value for aggregator");
    }

    @Override
    public void aggregateValue(V value) {
        this.checkValue(value);
        this.combiner.combine(value, this.value, this.value);
    }

    @Override
    public void aggregateValue(int value) {
        assert this.type == ValueType.INT;
        V localValue = this.localValue.get();
        ((IntValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    @Override
    public void aggregateValue(long value) {
        assert this.type == ValueType.LONG;
        V localValue = this.localValue.get();
        ((LongValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    @Override
    public void aggregateValue(float value) {
        assert this.type == ValueType.FLOAT;
        V localValue = this.localValue.get();
        ((FloatValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    @Override
    public void aggregateValue(double value) {
        assert this.type == ValueType.DOUBLE;
        V localValue = this.localValue.get();
        ((DoubleValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    private void combineAndSwapIfNeeded(V localValue, V thisValue) {
        this.combiner.combine(localValue, thisValue, thisValue);
        localValue.assign(thisValue);
    }

    @Override
    public V aggregatedValue() {
        assert this.value != null;
        return this.value;
    }

    @Override
    public void aggregatedValue(V value) {
        this.checkValue(value);
        this.value = value;
    }

    private void checkValue(V value) {
        E.checkNotNull(value, "aggregator", "value");
        E.checkArgument(value.valueType() == this.type,
                        "Can't set %s value '%s' to %s aggregator",
                        value.valueType().string(), value, this.type.string());
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public Aggregator<V> copy() {
        DefaultAggregator<V> aggregator = new DefaultAggregator<>(
                                          null, this.type,
                                          this.combinerClass,
                                          this.value);
        // Ensure deep copy the value
        @SuppressWarnings("unchecked")
        V deepCopyValue = (V) this.value.copy();
        aggregator.value = deepCopyValue;
        aggregator.combiner = this.combiner;
        aggregator.localValue = this.localValue;
        return aggregator;
    }

    @Override
    public void repair(ComputerContext context) {
        try {
            this.combiner = this.combinerClass.newInstance();
        } catch (Exception e) {
            throw new ComputerException("Can't new instance from class: %s",
                                        e, this.combinerClass.getName());
        }

        this.localValue = ThreadLocal.withInitial(() -> {
            return this.newValue(context);
        });

        if (this.value == null) {
            this.value = this.newValue(context);
        }
    }

    private V newValue(ComputerContext context) {
        @SuppressWarnings("unchecked")
        V val = (V) context.graphFactory().createValue(this.type);
        return val;
    }
}
