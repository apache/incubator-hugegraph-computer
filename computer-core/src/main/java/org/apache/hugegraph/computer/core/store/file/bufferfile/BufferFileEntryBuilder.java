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

package org.apache.hugegraph.computer.core.store.file.bufferfile;

import java.io.File;
import java.io.IOException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public class BufferFileEntryBuilder implements KvEntryFileWriter {

    private final RandomAccessOutput output;

    public BufferFileEntryBuilder(String path) {
        try {
            this.output = IOFactory.createFileOutput(new File(path));
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public void write(KvEntry entry) throws IOException {
        entry.key().write(this.output);
        entry.value().write(this.output);
    }

    @Override
    public void finish() throws IOException {
        this.close();
    }

    @Override
    public void close() throws IOException {
        this.output.close();
    }
}
