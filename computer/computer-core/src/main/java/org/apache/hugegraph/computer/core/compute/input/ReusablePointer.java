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

package org.apache.hugegraph.computer.core.compute.input;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.computer.core.util.BytesUtil;

public class ReusablePointer implements Pointer, Readable {

    private int length;
    private byte[] bytes;
    private RandomAccessInput input;

    public ReusablePointer() {
        this.bytes = Constants.EMPTY_BYTES;
        this.length = 0;
        this.input = IOFactory.createBytesInput(this.bytes);
    }

    public ReusablePointer(byte[] bytes, int length) {
        this.bytes = bytes;
        this.length = length;
        this.input = IOFactory.createBytesInput(this.bytes);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.length = in.readFixedInt();
        if (this.bytes.length < this.length) {
            this.bytes = new byte[this.length];
            this.input = IOFactory.createBytesInput(this.bytes);
        }
        in.readFully(this.bytes, 0, this.length);
    }

    @Override
    public RandomAccessInput input() {
        try {
            this.input.seek(0L);
        } catch (IOException e) {
            throw new ComputerException(
                      "ResuablePointer can't seek to position 0", e);
        }
        return this.input;
    }

    /**
     * Only [0 .. length) of the returned byte array is valid. The extra data
     * [length .. bytes.length) is meaningless, may be left by previous pointer.
     */
    @Override
    public byte[] bytes() {
        return this.bytes;
    }

    @Override
    public void write(RandomAccessOutput output) throws IOException {
        output.writeFixedInt(this.length);
        output.write(this.bytes(), 0, this.length);
    }

    @Override
    public long offset() {
        return 0L;
    }

    @Override
    public long length() {
        return this.length;
    }

    @Override
    public int compareTo(Pointer other) {
        try {
            return BytesUtil.compare(this.bytes(), this.length,
                                     other.bytes(), (int) other.length());
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }
}
