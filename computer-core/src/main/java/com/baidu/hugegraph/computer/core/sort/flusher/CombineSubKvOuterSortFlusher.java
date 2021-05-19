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
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.sort.sorter.SubKvSorter;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.util.E;

public class CombineSubKvOuterSortFlusher implements OuterSortFlusher {

    private final Combiner<Pointer> combiner;
    private final UnsafeBytesOutput output;
    private final int subKvFlushThreshold;
    private int sources;

    public CombineSubKvOuterSortFlusher(Combiner<Pointer> combiner,
                                        int subKvFlushThreshold) {
        this.combiner = combiner;
        this.output = new UnsafeBytesOutput();
        this.subKvFlushThreshold = subKvFlushThreshold;
    }

    @Override
    public void sources(int sources) {
        this.sources = sources;
    }

    @Override
    public void flush(Iterator<KvEntry> entries, HgkvDirBuilder writer)
                      throws IOException {
        E.checkArgument(entries.hasNext(),
                        "Parameter entries must not be empty");

        PeekableIterator<KvEntry> kvEntries = PeekableIteratorAdaptor.of(
                                              entries);
        SubKvSorter sorter = new SubKvSorter(kvEntries, this.sources);
        KvEntry currentKv = sorter.currentKv();

        while (true) {
            currentKv.key().write(this.output);
            long position = this.output.position();
            // Write value length placeholder
            this.output.writeInt(0);
            // Write subKv count placeholder
            this.output.writeInt(0);
            int writtenCount = 0;

            // Iterate subKv of currentKv
            KvEntry lastSubKv = sorter.next();
            Pointer lastSubValue = lastSubKv.value();
            while (true) {
                KvEntry current = null;
                if (sorter.hasNext()) {
                    current = sorter.next();
                    if (lastSubKv.compareTo(current) == 0) {
                        lastSubValue = this.combiner.combine(lastSubValue,
                                                             current.value());
                        continue;
                    }
                }

                lastSubKv.key().write(this.output);
                lastSubValue.write(this.output);
                writtenCount++;

                /*
                 * Fill placeholder if the number of subkvs with different
                 * keys is equal to the subKvFlushThreshold.
                 */
                if (current == null ||
                    writtenCount == this.subKvFlushThreshold) {
                    long currentPosition = this.output.position();
                    this.output.seek(position);
                    this.output.writeInt((int)(currentPosition - position - 4));
                    this.output.writeInt(writtenCount);
                    this.output.seek(currentPosition);
                    // Write kvEntry to file.
                    RandomAccessInput input = EntriesUtil.inputFromOutput(
                                                          this.output);
                    writer.write(EntriesUtil.entryFromInput(input, true));
                    this.output.seek(0);

                    if (current == null) {
                        break;
                    }
                    currentKv.key().write(this.output);
                    position = this.output.position();
                    // Write value length placeholder
                    this.output.writeInt(0);
                    // Write subKv count placeholder
                    this.output.writeInt(0);
                    writtenCount = 0;
                }

                lastSubKv = current;
                lastSubValue = lastSubKv.value();
            }
            sorter.reset();
            // Get next KV
            if ((currentKv = sorter.currentKv()) == null) {
                break;
            }
        }
        writer.finish();
    }
}
