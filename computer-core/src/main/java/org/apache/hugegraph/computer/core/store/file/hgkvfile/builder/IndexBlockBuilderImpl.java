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

package org.apache.hugegraph.computer.core.store.file.hgkvfile.builder;

import java.io.IOException;

import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

public class IndexBlockBuilderImpl implements IndexBlockBuilder {

    private final RandomAccessOutput output;
    private long blockSize;

    public IndexBlockBuilderImpl(RandomAccessOutput output) {
        this.output = output;
        this.blockSize = 0L;
    }

    @Override
    public void add(byte[] index) throws IOException {
        this.output.writeFixedInt(index.length);
        this.output.write(index);
        this.blockSize += Integer.SIZE + index.length;
    }

    @Override
    public void finish() {
        // pass
    }

    @Override
    public long length() {
        return this.blockSize;
    }
}
