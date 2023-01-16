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

package org.apache.hugegraph.computer.core.store.entry;

public abstract class AbstractKvEntry implements KvEntry {

    protected final Pointer key;
    protected final Pointer value;

    public AbstractKvEntry(Pointer key, Pointer value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public Pointer key() {
        return this.key;
    }

    @Override
    public Pointer value() {
        return this.value;
    }

    @Override
    public int compareTo(KvEntry o) {
        return this.key.compareTo(o.key());
    }
}
