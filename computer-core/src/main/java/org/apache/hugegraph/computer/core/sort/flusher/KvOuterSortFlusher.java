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

import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.util.E;

public class KvOuterSortFlusher implements OuterSortFlusher {

    @Override
    public void flush(EntryIterator entries, KvEntryFileWriter writer)
                      throws IOException {
        E.checkArgument(entries.hasNext(),
                        "Parameter entries can't be empty");
        while (entries.hasNext()) {
            KvEntry entry = entries.next();
            writer.write(entry);
        }
        writer.finish();
    }
}
