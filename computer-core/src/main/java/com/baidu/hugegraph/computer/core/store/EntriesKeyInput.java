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

package com.baidu.hugegraph.computer.core.store;

import java.io.IOException;
import java.util.ConcurrentModificationException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.store.entry.DefaultPointer;
import com.baidu.hugegraph.computer.core.store.entry.Pointer;

public class EntriesKeyInput implements InputIterator {

    private final RandomAccessInput input;
    private long position;

    public EntriesKeyInput(RandomAccessInput input) {
        this.input = input;
        this.position = 0;
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
    public Pointer next() {
        if (this.position != this.input.position()) {
            throw new ConcurrentModificationException();
        }

        int keyLength;
        long keyOffset;
        try {
            keyLength = this.input.readInt();
            keyOffset = this.input.position();
            this.input.skip(keyLength);
            this.input.skip(this.input.readInt());

            this.position = this.input.position();
            return new DefaultPointer(this.input, keyOffset, keyLength);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }
}
