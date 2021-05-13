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
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.util.E;

public class KvCombineOuterSortFlusher implements OuterSortFlusher {

    private final Combiner<Pointer> combiner;

    public KvCombineOuterSortFlusher(Combiner<Pointer> combiner) {
        this.combiner = combiner;
    }

    @Override
    public Combiner<Pointer> combiner() {
        return this.combiner;
    }

    @Override
    public void flush(Iterator<KvEntry> entries, HgkvDirBuilder writer)
                      throws IOException {
        E.checkArgument(entries.hasNext(),
                        "Parameter entries must not be empty.");

        KvEntry last = entries.next();
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

            Pointer key = sameKeyEntries.get(0).key();
            Pointer value = null;
            for (KvEntry entry : sameKeyEntries) {
                if (value == null) {
                    value = entry.value();
                    continue;
                }
                value = this.combiner.combine(value, entry.value());
            }

            assert value != null;
            writer.write(new DefaultKvEntry(key, value));

            if (current == null) {
                break;
            }

            sameKeyEntries.clear();
            sameKeyEntries.add(current);
            last = current;
        }
        writer.finish();
    }
}
