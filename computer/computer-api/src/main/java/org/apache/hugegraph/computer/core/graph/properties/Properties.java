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

package org.apache.hugegraph.computer.core.graph.properties;

import java.util.Map;

import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.io.Readable;
import org.apache.hugegraph.computer.core.io.Writable;

public interface Properties extends Readable, Writable {

    Map<String, Value> get();

    <T extends Value> T get(String key);

    void put(String key, Value value);

    void putIfAbsent(String key, Value value);

    void putAll(Map<String, Value> kvs);

    int size();

    void clear();
}
