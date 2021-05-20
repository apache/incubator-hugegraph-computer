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

package com.baidu.hugegraph.computer.core.sort.flusher;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.util.E;

public class PeekableIteratorAdaptor<T> implements PeekableIterator<T> {

    private final Iterator<T> entries;
    private T next;

    private PeekableIteratorAdaptor(Iterator<T> entries) {
        this.entries = entries;
        this.goNext();
    }

    public static <T> PeekableIterator<T> of(Iterator<T> iterator) {
        E.checkArgument(iterator.hasNext(),
                        "Parameter iterator must not be empty");
        return new PeekableIteratorAdaptor<>(iterator);
    }

    @Override
    public T peek() {
        return this.next;
    }

    @Override
    public boolean hasNext() {
        return this.next != null;
    }

    @Override
    public T next() {
        T next = this.next;

        this.goNext();

        return next;
    }

    private void goNext() {
        if (this.entries.hasNext()) {
            this.next = this.entries.next();
        } else {
            this.next = null;
        }
    }

    @Override
    public void close() throws Exception {
        if (this.entries instanceof AutoCloseable) {
            ((AutoCloseable) this.entries).close();
        }
    }

    @Override
    public Object metadata(String s, Object... objects) {
        if (this.entries instanceof CIter) {
            return ((CIter<T>) this.entries).metadata(s, objects);
        }
        throw new NotImplementedException();
    }
}
