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
import java.util.Iterator;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.OptimizedPointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;

public class MockOutSortFlusher implements OuterSortFlusher {

    private final UnsafeBytesOutput output;

    public MockOutSortFlusher() {
        this.output = new UnsafeBytesOutput();
    }

    @Override
    public Combiner<Pointer> combiner() {
        return null;
    }

    @Override
    public void sources(int sources) {
        // pass
    }

    @Override
    public void flush(Iterator<KvEntry> entries, HgkvDirBuilder writer)
                      throws IOException {
        if (!entries.hasNext()) {
            return;
        }

        int value = 0;
        KvEntry last = entries.next();
        value += StoreTestUtil.dataFromPointer(last.value());

        while (true) {
            KvEntry current = null;
            if (entries.hasNext()) {
                current = entries.next();
                if (last.compareTo(current) == 0) {
                    value += StoreTestUtil.dataFromPointer(current.value());
                    last = current;
                    continue;
                }
            }

            this.output.seek(0);
            this.output.writeInt(Integer.BYTES);
            this.output.write(last.key().bytes());
            this.output.writeInt(Integer.BYTES);
            this.output.writeInt(value);
            writer.write(this.entryFromOutput());

            if (current == null) {
                break;
            }

            last = current;
            value = StoreTestUtil.dataFromPointer(last.value());
        }
        writer.finish();
    }

    private KvEntry entryFromOutput() throws IOException {
        byte[] buffer = this.output.buffer();
        long position = this.output.position();
        RandomAccessInput input = new UnsafeBytesInput(buffer, position);
        int keyLength = input.readInt();
        long keyPosition = input.position();
        input.skip(keyLength);
        Pointer key = new OptimizedPointer(input, keyPosition, keyLength);
        int valueLength = input.readInt();
        long valuePosition = input.position();
        Pointer value = new OptimizedPointer(input, valuePosition, valueLength);
        return new DefaultKvEntry(key, value);
    }
}
