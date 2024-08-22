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

import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.io.Writable;

public class EntryOutputImpl implements EntryOutput {

    private final RandomAccessOutput output;
    private final boolean needSortSubKv;

    public EntryOutputImpl(RandomAccessOutput output,
                           boolean needSortSubKv) {
        this.output = output;
        this.needSortSubKv = needSortSubKv;
    }

    public EntryOutputImpl(RandomAccessOutput output) {
        this(output, true);
    }

    @Override
    public KvEntryWriter writeEntry(Writable key) throws IOException {
        // Write key
        this.writeData(key);
        return new KvEntryWriterImpl(this.output, this.needSortSubKv);
    }

    @Override
    public void writeEntry(Writable key, Writable value) throws IOException {
        // Write key
        this.writeData(key);
        // Write value
        this.writeData(value);
    }

    private void writeData(Writable data) throws IOException {
        // Write data length placeholder
        this.output.writeFixedInt(0);
        long position = this.output.position();
        // Write data
        data.write(this.output);
        // Fill data length placeholder
        int dataLength = (int) (this.output.position() - position);
        this.output.writeFixedInt(position - Integer.BYTES, dataLength);
    }
}
