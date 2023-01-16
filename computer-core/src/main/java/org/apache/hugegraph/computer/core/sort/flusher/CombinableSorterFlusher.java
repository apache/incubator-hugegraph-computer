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
import java.util.Iterator;

import org.apache.hugegraph.computer.core.combiner.PointerCombiner;
import org.apache.hugegraph.computer.core.store.entry.DefaultKvEntry;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.util.E;

public abstract class CombinableSorterFlusher {

    private final PointerCombiner combiner;

    public CombinableSorterFlusher(PointerCombiner combiner) {
        this.combiner = combiner;
    }

    public void flush(Iterator<KvEntry> entries) throws IOException {
        E.checkArgument(entries.hasNext(),
                        "Parameter entries can't be empty");

        KvEntry last = entries.next();
        Pointer combineValue = last.value();

        while (true) {
            KvEntry current = null;
            if (entries.hasNext()) {
                current = entries.next();
                if (last.compareTo(current) == 0) {
                    combineValue = this.combiner.combine(combineValue,
                                                         current.value());
                    continue;
                }
            }

            this.writeKvEntry(new DefaultKvEntry(last.key(), combineValue));

            if (current == null) {
                break;
            }

            last = current;
            combineValue = last.value();
        }
    }

    protected abstract void writeKvEntry(KvEntry entry) throws IOException;
}
