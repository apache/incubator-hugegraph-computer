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

package org.apache.hugegraph.computer.core.store.entry;

import java.io.IOException;

import org.apache.hugegraph.computer.core.io.RandomAccessInput;

public class EntryInputImpl implements EntryInput {

    private final RandomAccessInput input;

    public EntryInputImpl(RandomAccessInput input) {
        this.input = input;
    }

    @Override
    public KvEntryReader readEntry(Readable key) throws IOException {
        // Read key
        this.readData(key);
        return new KvEntryReaderImpl(this.input);
    }

    @Override
    public void readEntry(Readable key, Readable value) throws IOException {
        // Read key
        this.readData(key);
        // Read data
        this.readData(value);
    }

    private void readData(Readable data) throws IOException {
        // Read data length
        this.input.readFixedInt();
        // Read data
        data.read(this.input);
    }
}
