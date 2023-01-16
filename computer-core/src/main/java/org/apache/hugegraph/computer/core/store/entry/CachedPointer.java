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

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.util.BytesUtil;

public class CachedPointer implements Pointer {

    private final RandomAccessInput input;
    private final long offset;
    private final long length;
    private byte[] bytes;

    public CachedPointer(RandomAccessInput input, long offset, long length) {
        this.input = input;
        this.offset = offset;
        this.length = length;
        this.bytes = null;
    }

    @Override
    public RandomAccessInput input() {
        return this.input;
    }

    @Override
    public long offset() {
        return this.offset;
    }

    @Override
    public long length() {
        return this.length;
    }

    @Override
    public byte[] bytes() throws IOException {
        if (this.bytes == null) {
            this.input.seek(this.offset);
            this.bytes = this.input.readBytes((int) this.length);
        }
        return this.bytes;
    }

    @Override
    public void write(RandomAccessOutput output) throws IOException {
        output.writeFixedInt((int) this.length);
        output.write(this.bytes());
    }

    @Override
    public int compareTo(Pointer other) {
        try {
            return BytesUtil.compare(this.bytes(), other.bytes());
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }
}
