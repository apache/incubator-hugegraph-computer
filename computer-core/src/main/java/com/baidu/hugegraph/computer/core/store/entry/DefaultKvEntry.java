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

package com.baidu.hugegraph.computer.core.store.entry;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class DefaultKvEntry implements KvEntry {

    private final Pointer key;
    private final List<Pointer> values;

    public DefaultKvEntry(Pointer key, ImmutableList<Pointer> values) {
        this.key = key;
        this.values = values;
    }

    public DefaultKvEntry(Pointer key, List<Pointer> values) {
        this(key, ImmutableList.copyOf(values));
    }

    @Override
    public Pointer key() {
        return this.key;
    }

    @Override
    public Iterator<Pointer> values() {
        return this.values.iterator();
    }
}
