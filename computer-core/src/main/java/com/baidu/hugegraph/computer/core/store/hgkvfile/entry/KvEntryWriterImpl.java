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

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.computer.core.sort.sorter.InputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.JavaInputSorter;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesInput;

public class KvEntryWriterImpl implements KvEntryWriter {

    private final RandomAccessOutput output;
    private final long placeholderPosition;
    private final boolean needSort;
    private long total;
    private int subEntryCount;

    private final UnsafeBytesOutput subKvBuffer;

    public KvEntryWriterImpl(RandomAccessOutput output, boolean needSort) {
        this.output = output;
        this.placeholderPosition = output.position();
        try {
            // Write total subKv length placeholder
            this.output.writeInt(0);
            // Write total subKv count placeholder
            this.output.writeInt(0);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
        this.needSort = needSort;
        this.total = 0;
        this.subEntryCount = 0;

        if (needSort) {
            this.subKvBuffer = new UnsafeBytesOutput();
        } else {
            this.subKvBuffer = null;
        }
    }

    @Override
    public void writeSubKv(Writable subKey, Writable subValue)
                           throws IOException {
        this.writeData(subKey);
        this.writeData(subValue);
        this.subEntryCount++;
    }

    @Override
    public void writeFinish() throws IOException {
        // Fill total value length
        this.output.writeInt(this.placeholderPosition,
                             (int) this.total + Integer.BYTES);
        // Fill sub-entry count
        this.output.writeInt(this.placeholderPosition + Integer.BYTES,
                             this.subEntryCount);

        if (this.needSort) {
            // Sort subKvs
            this.sortAndWriteSubKvs();
        }
    }

    private void sortAndWriteSubKvs() throws IOException {
        UnsafeBytesInput input = EntriesUtil.inputFromOutput(this.subKvBuffer);
        InputSorter sorter = new JavaInputSorter();
        Iterator<KvEntry> subKvs = sorter.sort(new KvEntriesInput(input));

        while (subKvs.hasNext()) {
            KvEntry subKv = subKvs.next();
            subKv.key().write(this.output);
            subKv.value().write(this.output);
        }
    }

    private void writeData(Writable data) throws IOException {
        RandomAccessOutput output;
        if (this.needSort) {
            assert this.subKvBuffer != null;
            output = this.subKvBuffer;
        } else {
            output = this.output;
        }

        long position = output.position();
        // Write data length placeholder
        output.writeInt(0);
        // Write data
        data.write(output);
        // Fill data length placeholder
        long dataLength = output.position() - position - Integer.BYTES;
        output.writeInt(position, (int) dataLength);
        this.total += Integer.BYTES + dataLength;
    }
}
