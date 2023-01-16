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

import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.entry.Pointer;

public interface OuterSortFlusher {

    /**
     * Number of path to generate entries iterator in flush method.
     * This method must be called before flush if sort with subKv.
     */
    default void sources(int sources) {
        throw new NotImplementedException();
    }

    /**
     * Combiner entries with the same key.
     */
    default Combiner<Pointer> combiner() {
        throw new NotImplementedException();
    }

    /**
     * Combine the list of inputValues, and write the inputKey and combined
     * result length and results to HgkvDirWriter.
     * The caller maybe needs to call the sources method before call this
     * method.
     */
    void flush(EntryIterator entries, KvEntryFileWriter writer)
               throws IOException;
}
