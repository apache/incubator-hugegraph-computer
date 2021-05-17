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

package com.baidu.hugegraph.computer.core.store.hgkvfile.entry;

import java.io.IOException;
import java.util.Iterator;

import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.computer.core.sort.sorter.InputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.JavaInputSorter;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntriesInput;

public class KvEntryWriterImpl implements KvEntryWriter {

    private final RandomAccessOutput output;
    private long total;
    private int subEntryCount;

    private final UnsafeBytesOutput subKvBuffer;

    public KvEntryWriterImpl(RandomAccessOutput output) {
        this.output = output;
        this.total = 0;
        this.subEntryCount = 0;

        this.subKvBuffer = new UnsafeBytesOutput();
    }

    @Override
    public void writeSubKey(Writable subKey) throws IOException {
        this.writeData(subKey);
    }

    @Override
    public void writeSubValue(Writable subValue) throws IOException {
        this.writeData(subValue);
        this.subEntryCount++;
    }

    @Override
    public void writeFinish() throws IOException {
        // Write total value length
        this.output.writeInt((int) this.total);
        // Write sub-entry count
        this.output.writeInt(this.subEntryCount);

        // Sort subKvs
        this.sortSubKvs();
    }

    private void sortSubKvs() throws IOException {
        UnsafeBytesInput input = EntriesUtil.inputFromOutput(this.subKvBuffer);
        InputSorter sorter = new JavaInputSorter();
        Iterator<KvEntry> subKvs = sorter.sort(new EntriesInput(input));

        while (subKvs.hasNext()) {
            KvEntry subKv = subKvs.next();
            subKv.key().write(this.output);
            subKv.value().write(this.output);
        }
    }

    private void writeData(Writable data) throws IOException {
        long position = this.subKvBuffer.position();
        // Write data length placeholder
        this.subKvBuffer.writeInt(0);
        // Write data
        data.write(this.subKvBuffer);
        // Fill data length placeholder
        int dataLength = (int) (this.subKvBuffer.position() - position -
                                Integer.BYTES);
        this.subKvBuffer.writeInt(position, dataLength);
        this.total += Integer.BYTES + dataLength;
    }
}
