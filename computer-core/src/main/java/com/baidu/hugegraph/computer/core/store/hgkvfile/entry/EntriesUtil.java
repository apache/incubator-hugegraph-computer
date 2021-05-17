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
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;

public final class EntriesUtil {

    private static final int DEFAULT_CAPACITY = 100000;

    public static List<KvEntry> readInput(RandomAccessInput input) {
        List<KvEntry> pointers = new ArrayList<>(DEFAULT_CAPACITY);
        EntriesInput entriesInput = new EntriesInput(input);
        while (entriesInput.hasNext()) {
            pointers.add(entriesInput.next());
        }
        return pointers;
    }

    public static EntryIterator subKvIterFromEntry(KvEntry entry) {
        return new SubKvIterator(entry);
    }

    private static class SubKvIterator implements EntryIterator {

        private final RandomAccessInput input;
        private final RandomAccessInput userAccessInput;
        private long size;

        public SubKvIterator(KvEntry kvEntry) {
            try {
                this.input = new UnsafeBytesInput(kvEntry.value().bytes());
                this.userAccessInput = this.input.duplicate();
                this.size = this.input.readInt();
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }
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
            return EntriesUtil.entryFromInput(this.input, this.userAccessInput);
        }

        @Override
        public void close() throws Exception {
            this.input.close();
            this.userAccessInput.close();
        }
    }

    public static KvEntry entryFromInput(RandomAccessInput input) {
        return entryFromInput(input, input);
    }

    public static KvEntry entryFromInput(RandomAccessInput input,
                                         RandomAccessInput userAccessInput) {
        try {
            // Read key
            int keyLength = input.readInt();
            long keyOffset = input.position();
            input.skip(keyLength);

            // Read value
            int valueLength = input.readInt();
            long valueOffset = input.position();
            input.skip(valueLength);

            Pointer key = new InlinePointer(userAccessInput, keyOffset,
                                            keyLength);
            Pointer value = new InlinePointer(userAccessInput, valueOffset,
                                              valueLength);
            return new DefaultKvEntry(key, value);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public static KvEntryWithFirstSubKv kvEntryWithFirstSubKv(KvEntry entry) {
        try {
            RandomAccessInput input = entry.value().input();
            input.seek(entry.value().offset());
            // Skip sub-entry size
            input.skip(Integer.BYTES);
            KvEntry firstSubKv = EntriesUtil.entryFromInput(input);

            return new KvEntryWithFirstSubKv(entry.key(), entry.value(),
                                             firstSubKv);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public static UnsafeBytesInput inputFromOutput(UnsafeBytesOutput output) {
        return new UnsafeBytesInput(output.buffer(), output.position());
    }
}
