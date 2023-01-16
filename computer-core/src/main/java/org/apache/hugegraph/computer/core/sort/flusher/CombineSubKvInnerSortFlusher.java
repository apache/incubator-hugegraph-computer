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

package org.apache.hugegraph.computer.core.sort.flusher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.core.combiner.PointerCombiner;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.sort.sorting.SortingFactory;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.util.E;

public class CombineSubKvInnerSortFlusher implements InnerSortFlusher {

    private final RandomAccessOutput output;
    private final PointerCombiner combiner;
    private final int subKvFlushThreshold;

    public CombineSubKvInnerSortFlusher(RandomAccessOutput output,
                                        PointerCombiner combiner,
                                        int subKvFlushThreshold) {
        this.output = output;
        this.combiner = combiner;
        this.subKvFlushThreshold = subKvFlushThreshold;
    }

    @Override
    public RandomAccessOutput output() {
        return this.output;
    }

    @Override
    public PointerCombiner combiner() {
        return this.combiner;
    }

    @Override
    public void flush(Iterator<KvEntry> entries) throws IOException {
        E.checkArgument(entries.hasNext(), "Parameter entries can't be empty");

        KvEntry last = entries.next();
        // TODO: use byte buffer store all value pointer to avoid big collection
        List<KvEntry> sameKeyEntries = new ArrayList<>();
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

            this.writeSubKvs(last, this.sortedSubKvFromEntries(sameKeyEntries));

            if (current == null) {
                break;
            }

            sameKeyEntries.clear();
            sameKeyEntries.add(current);
            last = current;
        }
    }

    private Iterator<KvEntry> sortedSubKvFromEntries(List<KvEntry> entries) {
        Function<KvEntry, Iterator<KvEntry>> kvEntryToSubKvs =
                                             EntriesUtil::subKvIterFromEntry;
        List<Iterator<KvEntry>> subKvs = entries.stream()
                                                .map(kvEntryToSubKvs)
                                                .collect(Collectors.toList());

        return SortingFactory.createSorting(subKvs);
    }

    private void writeSubKvs(KvEntry kvEntry, Iterator<KvEntry> subKvIter)
                             throws IOException {
        E.checkArgument(subKvIter.hasNext(),
                        "Parameter subKvs can't be empty");

        kvEntry.key().write(this.output);
        long position = this.output.position();
        // Write value length placeholder
        this.output.writeFixedInt(0);
        // Write subKv count placeholder
        this.output.writeFixedInt(0);

        // Write subKv to output
        KvEntry lastSubKv = subKvIter.next();
        Pointer lastSubValue = lastSubKv.value();
        int writtenCount = 0;

        while (true) {
            // Write subKv
            KvEntry current = null;
            if (subKvIter.hasNext()) {
                current = subKvIter.next();
                if (lastSubKv.compareTo(current) == 0) {
                    lastSubValue = this.combiner.combine(lastSubValue,
                                                         current.value());
                    continue;
                }
            }

            lastSubKv.key().write(this.output);
            lastSubValue.write(this.output);
            writtenCount++;

            if (writtenCount == this.subKvFlushThreshold || current == null) {
                // Fill placeholder
                long currentPosition = this.output.position();
                this.output.seek(position);
                // Fill value length placeholder
                this.output.writeFixedInt((int)
                                          (currentPosition - position - 4));
                // Fill subKv count placeholder
                this.output.writeFixedInt(writtenCount);
                this.output.seek(currentPosition);

                if (current == null) {
                    break;
                }

                // Used for next loop
                kvEntry.key().write(this.output);
                position = this.output.position();
                // Write value length placeholder
                this.output.writeFixedInt(0);
                // Write subKv count placeholder
                this.output.writeFixedInt(0);
                writtenCount = 0;
            }

            lastSubKv = current;
            lastSubValue = current.value();
        }
    }
}
