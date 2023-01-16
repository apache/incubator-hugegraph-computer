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

package org.apache.hugegraph.computer.core.store.buffer;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public class KvEntriesInput implements EntryIterator {

    private final RandomAccessInput input;
    private final boolean withSubKv;
    private final RandomAccessInput userAccessInput;

    public KvEntriesInput(RandomAccessInput input, boolean withSubKv) {
        this.input = input;
        this.withSubKv = withSubKv;
        try {
            this.userAccessInput = this.input.duplicate();
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public KvEntriesInput(RandomAccessInput input) {
        this(input, false);
    }

    @Override
    public boolean hasNext() {
        try {
            return this.input.available() > 0;
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public KvEntry next() {
        return EntriesUtil.kvEntryFromInput(this.input, this.userAccessInput,
                                            true, this.withSubKv);
    }

    @Override
    public void close() throws IOException {
        this.input.close();
        this.userAccessInput.close();
    }
}
