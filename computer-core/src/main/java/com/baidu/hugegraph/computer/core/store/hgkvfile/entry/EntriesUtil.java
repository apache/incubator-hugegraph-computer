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

    public static List<KvEntry> readInput(RandomAccessInput input)
                                          throws IOException {
        List<KvEntry> pointers = new ArrayList<>(DEFAULT_CAPACITY);
        try (EntriesInput entriesInput = new EntriesInput(input)) {
            while (entriesInput.hasNext()) {
                pointers.add(entriesInput.next());
            }
        }

        return pointers;
    }

    public static EntryIterator subKvIterFromEntry(KvEntry entry) {
        return new SubKvIterator(entry);
    }

    private static class SubKvIterator implements EntryIterator {

        private final RandomAccessInput input;
        private final RandomAccessInput useAccessInput;
        private long size;
        private final boolean useInlinePointer;

        public SubKvIterator(KvEntry kvEntry, boolean useInlinePointer) {
            try {
                this.input = new UnsafeBytesInput(kvEntry.value().bytes());
                this.useAccessInput = this.input.duplicate();
                this.size = this.input.readInt();
                this.useInlinePointer = useInlinePointer;
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }
        }

        public SubKvIterator(KvEntry kvEntry) {
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
            return EntriesUtil.entryFromInput(this.input, this.useAccessInput,
                                              this.useInlinePointer, true);
        }

        @Override
        public void close() throws Exception {
            this.input.close();
            this.useAccessInput.close();
        }
    }

    public static KvEntry entryFromInput(RandomAccessInput input,
                                         RandomAccessInput userAccessInput,
                                         boolean useInlinePointer,
                                         boolean valueWithSubKv) {
        try {
            if (useInlinePointer) {
                return inlinePointerKvEntry(input, valueWithSubKv);
            } else {
                return cachedPointerKvEntry(input, userAccessInput,
                                            valueWithSubKv);
            }
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public static KvEntry entryFromInput(RandomAccessInput input,
                                         boolean useInlinePointer,
                                         boolean valueWithSubKv) {
        return entryFromInput(input, input, useInlinePointer, valueWithSubKv);
    }
    
    private static KvEntry cachedPointerKvEntry(
                           RandomAccessInput input,
                           RandomAccessInput userAccessInput,
                           boolean valueWithSubKv)
                           throws IOException {
        int numSubKvEntries = 0;

        // Read key
        int keyLength = input.readInt();
        long keyPosition = input.position();
        input.skip(keyLength);

        // Read value
        int valueLength = input.readInt();
        long valuePosition = input.position();
        if (valueWithSubKv) {
            numSubKvEntries = input.readInt();
            input.skip(valueLength - Integer.BYTES);
        } else {
            input.skip(valueLength);
        }

        Pointer key = new CachedPointer(userAccessInput, keyPosition,
                                        keyLength);
        Pointer value = new CachedPointer(userAccessInput, valuePosition,
                                          valueLength);
        
        return new DefaultKvEntry(key, value, numSubKvEntries);
    }

    private static KvEntry inlinePointerKvEntry(RandomAccessInput input,
                                                boolean valueWithSubKv)
                                                throws IOException {
        int numSubEntries = 0;
        // Read key
        int keyLength = input.readInt();
        byte[] keyBytes = input.readBytes(keyLength);

        // Read value
        int valueLength = input.readInt();
        byte[] valueBytes = new byte[valueLength];
        if (valueWithSubKv) {
            numSubEntries = input.readInt();
            valueBytes[0] = (byte) (numSubEntries & 0xFF);
            valueBytes[1] = (byte) ((numSubEntries >> 8) & 0xFF);
            valueBytes[2] = (byte) ((numSubEntries >> 16) & 0xFF);
            valueBytes[3] = (byte) ((numSubEntries >> 24) & 0xFF);
            input.readFully(valueBytes, 4, valueLength - 4);
        } else {
            input.readFully(valueBytes);
        }

        Pointer key = new InlinePointer(keyBytes);
        Pointer value = new InlinePointer(valueBytes);

        return new DefaultKvEntry(key, value, numSubEntries);
    }

    public static KvEntryWithFirstSubKv kvEntryWithFirstSubKv(KvEntry entry) {
        try {
            RandomAccessInput input = new UnsafeBytesInput(
                                      entry.value().bytes());
            // Skip sub-entry size
            input.skip(Integer.BYTES);
            KvEntry firstSubKv = EntriesUtil.entryFromInput(input, true, true);

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
