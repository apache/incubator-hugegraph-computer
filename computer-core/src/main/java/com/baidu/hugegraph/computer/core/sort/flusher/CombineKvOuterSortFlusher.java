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
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.util.E;

public class CombineKvOuterSortFlusher implements OuterSortFlusher {

    private final Combiner<Pointer> combiner;

    public CombineKvOuterSortFlusher(Combiner<Pointer> combiner) {
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
                        "Parameter entries must not be empty");

        KvEntry last = entries.next();
        Pointer value = last.value();

        while (true) {
            KvEntry current = null;
            if (entries.hasNext()) {
                current = entries.next();
                if (last.compareTo(current) == 0) {
                    value = this.combiner.combine(value, current.value());
                    continue;
                }
            }

            writer.write(new DefaultKvEntry(last.key(), value));

            if (current == null) {
                break;
            }

            last = current;
            value = last.value();
        }
        writer.finish();
    }
}
