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

package com.baidu.hugegraph.computer.core.aggregator;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.util.E;

public class DefaultAggregator<V extends Value<?>> implements Aggregator<V> {

    private final ValueType type;
    private final Class<? extends Combiner<V>> combinerClass;

    private transient Combiner<V> combiner;
    private transient ThreadLocal<V> localValue;

    private V value;

    public DefaultAggregator(ComputerContext context, ValueType type,
                             Class<? extends Combiner<V>> combinerClass) {
        this.type = type;
        this.combinerClass = combinerClass;

        if (context != null) {
            this.repair(context);
        }
    }

    @Override
    public void aggregateValue(V value) {
        E.checkNotNull(value, "aggregator", "value");
        this.value = this.combiner.combine(value, this.value);
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
        V localValue= this.localValue.get();
        ((LongValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    @Override
    public void aggregateValue(float value) {
        assert this.type == ValueType.FLOAT;
        V localValue= this.localValue.get();
        ((FloatValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    @Override
    public void aggregateValue(double value) {
        assert this.type == ValueType.DOUBLE;
        V localValue= this.localValue.get();
        ((DoubleValue) localValue).value(value);
        this.combineAndSwapIfNeeded(localValue, this.value);
    }

    private void combineAndSwapIfNeeded(V localValue, V value2) {
        V tmp = this.combiner.combine(localValue, this.value);
        if (tmp == localValue) {
            this.localValue.set(this.value);
        }
        this.value = tmp;
    }

    @Override
    public V aggregatedValue() {
        assert this.value != null;
        return this.value;
    }

    @Override
    public void aggregatedValue(V value) {
        E.checkNotNull(value, "aggregator", "value");
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public Aggregator<V> copy() {
        DefaultAggregator<V> aggregator = new DefaultAggregator<>(
                                          null, this.type, this.combinerClass);
        // Ensure deep copy the value
        @SuppressWarnings("unchecked")
        V deepCopyValue = (V) this.value.copy();
        aggregator.value = deepCopyValue;
        aggregator.combiner = this.combiner;
        aggregator.localValue = this.localValue;
        return aggregator;
//        try {
//            @SuppressWarnings("unchecked")
//            Aggregator<V> aggregator = (Aggregator<V>) this.clone();
//            return aggregator;
//        } catch (CloneNotSupportedException e) {
//            throw new ComputerException("Failed to copy Aggregator", e);
//        }
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
        V val = (V) context.valueFactory().createValue(this.type);
        return val;
    }
}
