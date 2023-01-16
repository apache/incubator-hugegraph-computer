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

package org.apache.hugegraph.computer.core.store.buffer;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public class SubKvEntriesInput implements EntryIterator {

    private final RandomAccessInput input;
    private final RandomAccessInput useAccessInput;
    private int size;
    private final boolean useInlinePointer;

    public SubKvEntriesInput(KvEntry kvEntry, boolean useInlinePointer) {
        try {
            this.input = IOFactory.createBytesInput(kvEntry.value().bytes());
            this.useAccessInput = this.input.duplicate();
            this.size = this.input.readFixedInt();
            this.useInlinePointer = useInlinePointer;
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public SubKvEntriesInput(KvEntry kvEntry) {
        this(kvEntry, true);
    }

    @Override
    public boolean hasNext() {
        return this.size > 0;
    }

    @Override
    public KvEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        this.size--;
        return EntriesUtil.subKvEntryFromInput(this.input, this.useAccessInput,
                                               this.useInlinePointer);
    }

    @Override
    public void close() throws Exception {
        this.input.close();
        this.useAccessInput.close();
    }
}
