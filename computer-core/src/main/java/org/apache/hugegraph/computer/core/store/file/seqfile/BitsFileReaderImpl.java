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

package org.apache.hugegraph.computer.core.store.file.seqfile;

import java.io.File;
import java.io.IOException;

import org.apache.hugegraph.computer.core.config.Config;

public class BitsFileReaderImpl implements BitsFileReader {

    private static final int BUFFER_BITS = Long.BYTES << 3;

    private final ValueFileInput input;
    private boolean closed;
    private long byteBuffer;
    private int cursor;

    public BitsFileReaderImpl(Config config, String path) throws IOException {
        this(config, new File(path));
    }

    public BitsFileReaderImpl(Config config, File file) throws IOException {
        this.input = new ValueFileInput(config, file);
        this.closed = false;

        this.byteBuffer = this.input.readLong();
        this.cursor = 0;
    }

    @Override
    public boolean readBoolean() throws IOException {
        if (this.cursor >= BUFFER_BITS) {
            this.byteBuffer = this.input.readLong();
            this.cursor = 0;
        }

        return (this.byteBuffer >> this.cursor++ & 1) == 1;
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.input.close();
    }
}
