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
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.E;

public class DefaultAggregator<V extends Value<?>> implements Aggregator<V> {

    private final ValueType type;
    private final Class<? extends Combiner<V>> combinerClass;

    private final transient ComputerContext context;
    private final transient Combiner<V> combiner;
    private final transient ThreadLocal<V> combineValue;

    private V value;

    public DefaultAggregator(ComputerContext context, ValueType type,
                             Class<? extends Combiner<V>> combinerClass) {
        this.type = type;
        this.combinerClass = combinerClass;

        this.context = context;
        try {
            this.combiner = this.combinerClass.newInstance();
        } catch (Exception e) {
            throw new ComputerException("Can't new instance from class: %s",
                                        e, this.combinerClass.getName());
        }

        this.combineValue = ThreadLocal.withInitial(() -> {
            return this.newValue(context);
        });

        this.value = this.newValue(context);
    }

    private V newValue(ComputerContext context) {
        @SuppressWarnings("unchecked")
        V val = (V) context.valueFactory().createValue(this.type);
        return val;
    }

    @Override
    public void aggregateValue(V value) {
        E.checkNotNull(value, "aggregator", "value");
        this.value = this.combiner.combine(value, this.value);
    }

    @Override
    public void aggregateValue(int value) {
        assert this.type == ValueType.INT;
        V combineValue= this.combineValue.get();
        // NOTE: the Value class must provide value(int) method to set value
        Whitebox.invoke(combineValue.getClass(), "value", combineValue, value);
        this.value = this.combiner.combine(combineValue, this.value);
    }

    @Override
    public void aggregateValue(long value) {
        assert this.type == ValueType.LONG;
        V combineValue= this.combineValue.get();
        // NOTE: the Value class must provide value(long) method to set value
        Whitebox.invoke(combineValue.getClass(), "value", combineValue, value);
        this.value = this.combiner.combine(combineValue, this.value);
    }

    @Override
    public void aggregateValue(float value) {
        assert this.type == ValueType.FLOAT;
        V combineValue= this.combineValue.get();
        // NOTE: the Value class must provide value(float) method to set value
        Whitebox.invoke(combineValue.getClass(), "value", combineValue, value);
        this.value = this.combiner.combine(combineValue, this.value);
    }

    @Override
    public void aggregateValue(double value) {
        assert this.type == ValueType.DOUBLE;
        V combineValue= this.combineValue.get();
        // NOTE: the Value class must provide value(double) method to set value
        Whitebox.invoke(combineValue.getClass(), "value", combineValue, value);
        this.value = this.combiner.combine(combineValue, this.value);
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
    public Aggregator<V> copy() {
        return new DefaultAggregator<>(this.context, this.type,
                                       this.combinerClass);
    }
}
