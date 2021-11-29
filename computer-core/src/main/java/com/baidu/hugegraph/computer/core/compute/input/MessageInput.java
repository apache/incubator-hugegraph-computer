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

package com.baidu.hugegraph.computer.core.compute.input;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.IdType;

public class MessageInput<T extends Value<?>> {

    private final Config config;
    private final PeekableIterator<KvEntry> messages;
    private T value;

    public MessageInput(ComputerContext context,
                        PeekableIterator<KvEntry> messages,
                        boolean inCompute) {
        if (messages == null) {
            this.messages = PeekableIterator.emptyIterator();
        } else {
            this.messages = messages;
        }
        this.config = context.config();

        if (!inCompute) {
            this.value = (T)(new IdList());
        }
        else {
            this.value = this.config.createObject(
                     ComputerOptions.ALGORITHM_MESSAGE_CLASS);
        }
    }

    public Iterator<T> iterator(long vid) {
        while (this.messages.hasNext()) {
            KvEntry entry = this.messages.peek();
            Pointer key = entry.key();
            try {
                byte[] bkey = key.bytes();
                int length = bkey[1];
                byte[] blId = new byte[length];
                for (int j = 0; j < length; j++) {
                    blId[j] = bkey[j + 2];
                }
                BytesId id = new BytesId(IdType.LONG, blId);
                long lid = (long)id.asObject();
                if (lid > vid) {
                    return Collections.emptyIterator();
                }
                else if (lid == vid) {
                    ReusablePointer vidPointer = 
                                    new ReusablePointer(bkey,
                                                     bkey.length);
                    return new MessageIterator(vidPointer);
                }
                else {
                    this.messages.next();
                }
            } catch (IOException e) {
                throw new ComputerException("read id error", e);
            }
        }
        return Collections.emptyIterator();
    }

    public Iterator<T> iterator(ReusablePointer vidPointer) {
        while (this.messages.hasNext()) {
            KvEntry entry = this.messages.peek();
            Pointer key = entry.key();
            Pointer value = entry.value();
            int status = vidPointer.compareTo(key);
            if (status < 0) {
                return Collections.emptyIterator();
            } else if (status == 0) {
                break;
            } else {
                this.messages.next();
            }
        }

        return new MessageIterator(vidPointer);
    }

    public void close() throws Exception {
        this.messages.close();
    }

    private class MessageIterator implements Iterator<T> {

        // It indicates whether the value can be returned to client.
        private boolean valueValid;
        private ReusablePointer vidPointer;

        private MessageIterator(ReusablePointer vidPointer) {
            this.vidPointer = vidPointer;
            this.valueValid = false;
        }

        @Override
        public boolean hasNext() {
            if (this.valueValid) {
                return true;
            }
            if (MessageInput.this.messages.hasNext()) {
                KvEntry entry = MessageInput.this.messages.peek();
                Pointer key = entry.key();
                int status = this.vidPointer.compareTo(key);
                if (status == 0) {
                    MessageInput.this.messages.next();
                    this.valueValid = true;
                    try {
                        BytesInput in = IOFactory.createBytesInput(
                                        entry.value().bytes());
                        MessageInput.this.value.read(in);
                    } catch (IOException e) {
                        throw new ComputerException("Can't read value", e);
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public T next() {
            if (this.valueValid) {
                this.valueValid = false;
                return MessageInput.this.value;
            } else {
                throw new NoSuchElementException();
            }
        }
    }
}
