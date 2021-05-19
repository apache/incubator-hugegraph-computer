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

package com.baidu.hugegraph.computer.core.store.hgkvfile.buffer;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;

public class EntriesInput implements EntryIterator {

    private final RandomAccessInput input;
    private final boolean userInlinePointer;
    private final RandomAccessInput userAccessInput;

    public EntriesInput(RandomAccessInput input, boolean useInlinePointer) {
        this.input = input;
        this.userInlinePointer = useInlinePointer;
        try {
            this.userAccessInput = this.input.duplicate();
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public EntriesInput(RandomAccessInput input) {
        this(input, true);
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
        return EntriesUtil.entryFromInput(this.input, this.userAccessInput,
                                          this.userInlinePointer);
    }

    @Override
    public void close() throws IOException {
        this.input.close();
        this.userAccessInput.close();
    }
}
