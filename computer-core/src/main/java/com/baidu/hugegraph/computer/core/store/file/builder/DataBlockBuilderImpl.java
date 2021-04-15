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

import java.io.IOException;

import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.store.base.Pointer;

public class DataBlockBuilderImpl implements BlockBuilder {

    private final RandomAccessOutput output;
    private long entriesSize;

    public DataBlockBuilderImpl(RandomAccessOutput output) {
        this.output = output;
        this.entriesSize = 0;
    }

    @Override
    public void add(Pointer key, Pointer value) throws IOException {
        this.writePointer(this.output, key);
        this.writePointer(this.output, value);
    }

    @Override
    public long sizeOfEntry(Pointer key, Pointer value) {
        long keyLength = Integer.BYTES + key.length();
        long valueLength = Integer.BYTES + value.length();
        return keyLength + valueLength;
    }

    @Override
    public long size() {
        return this.entriesSize;
    }

    @Override
    public void finish() {
        // Nothing to do.
    }

    @Override
    public void reset() {
        this.entriesSize = 0;
    }

    private void writePointer(RandomAccessOutput output, Pointer pointer)
                              throws IOException {
        output.writeInt((int) pointer.length());
        output.write(pointer.input(), pointer.offset(), pointer.length());
        this.entriesSize += (Integer.BYTES + pointer.length());
    }
}
