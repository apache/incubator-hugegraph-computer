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

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;

public class KvEntryReaderImpl implements KvEntryReader {

    private final RandomAccessInput input;
    private int remaining;

    public KvEntryReaderImpl(RandomAccessInput input) {
        this.input = input;
        try {
            @SuppressWarnings("unused")
            int totalLength = input.readFixedInt();
            this.remaining = input.readFixedInt();
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public void readSubKv(Readable subKey, Readable subValue)
                          throws IOException {
        this.readDataWithoutLength(subKey);
        this.readDataWithoutLength(subValue);
        this.remaining--;
    }

    @Override
    public boolean hasRemaining() throws IOException {
        return this.remaining > 0;
    }

    private void readDataWithoutLength(Readable data) throws IOException {
        // Read data directly
        data.read(this.input);
    }
}
