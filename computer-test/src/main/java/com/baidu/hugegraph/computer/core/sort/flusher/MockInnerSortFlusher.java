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

package com.baidu.hugegraph.computer.core.sort.flusher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;

public class MockInnerSortFlusher implements InnerSortFlusher {

    private final RandomAccessOutput output;

    public MockInnerSortFlusher(RandomAccessOutput output) {
        this.output = output;
    }

    @Override
    public RandomAccessOutput output() {
        return this.output;
    }

    @Override
    public Combiner<Pointer> combiner() {
        return null;
    }

    @Override
    public void flush(Iterator<KvEntry> entries) throws IOException {
        if (!entries.hasNext()) {
            return;
        }

        List<KvEntry> sameKeyEntries = new ArrayList<>();
        KvEntry last = entries.next();
        sameKeyEntries.add(last);

        while (true) {
            KvEntry current = null;
            if (entries.hasNext()) {
                current = entries.next();
                if (last.compareTo(current) == 0) {
                    sameKeyEntries.add(current);
                    continue;
                }
            }

            // Write same key
            Pointer key = last.key();
            this.output.writeInt((int) key.length());
            this.output.write(key.input(), key.offset(), key.length());
            // Write value length placeholder
            long position = this.output.position();
            this.output.writeInt(0);
            // Write values of the same key in sequence
            int valueLength = 0;
            for (KvEntry entry : sameKeyEntries) {
                Pointer value = entry.value();
                this.output.write(value.input(), value.offset(),
                                  value.length());
                valueLength += value.length();
            }
            // Fill value length placeholder
            this.output.writeInt(position, valueLength);

            if (current == null) {
                break;
            }

            last = current;
            sameKeyEntries.clear();
            sameKeyEntries.add(last);
        }
    }
}
