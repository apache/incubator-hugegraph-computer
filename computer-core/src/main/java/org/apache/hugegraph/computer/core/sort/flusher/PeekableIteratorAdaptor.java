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

package org.apache.hugegraph.computer.core.sort.flusher;

import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.util.E;

public class PeekableIteratorAdaptor<T> implements PeekableIterator<T> {

    private final CIter<T> entries;
    private T next;

    private PeekableIteratorAdaptor(CIter<T> entries) {
        this.entries = entries;
        this.fetchNext();
    }

    public static <T> PeekableIterator<T> of(CIter<T> iterator) {
        E.checkArgument(iterator.hasNext(),
                        "Parameter iterator can't be empty");
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

        this.fetchNext();

        return next;
    }

    private void fetchNext() {
        if (this.entries.hasNext()) {
            this.next = this.entries.next();
        } else {
            this.next = null;
        }
    }

    @Override
    public void close() throws Exception {
        this.entries.close();
    }

    @Override
    public Object metadata(String s, Object... objects) {
        return this.entries.metadata(s, objects);
    }
}
