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

package com.baidu.hugegraph.computer.core.store.file.builder;

import java.io.Closeable;
import java.io.IOException;

import com.baidu.hugegraph.computer.core.store.entry.Pointer;

public interface HgkvFileBuilder extends Closeable {

    /**
     * Add kv entry to file.
     */
    void add(Pointer key, Pointer value) throws IOException;

    /**
     * Return size of new entry.
     */
    long sizeOfEntry(Pointer key, Pointer value);

    /**
     * Finish build file.
     */
    void finish() throws IOException;

    /**
     * Returns the size of entry that has been written.
     */
    long dataSize();
}