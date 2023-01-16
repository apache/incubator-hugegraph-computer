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

package org.apache.hugegraph.computer.core.sort.sorting;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class HeapInputsSorting<T> extends AbstractInputsSorting<T> {

    private final Object[] data;
    private int size;

    public HeapInputsSorting(Collection<? extends Iterator<T>> sources) {
        this(sources, null);
    }

    public HeapInputsSorting(Collection<? extends Iterator<T>> sources,
                             Comparator<? super T> comparator) {
        super(sources, comparator);

        this.size = sources.size();
        this.data = new Object[this.size];

        // Init Heap
        this.constructHeap();
    }

    @Override
    public boolean hasNext() {
        return !this.isEmpty();
    }

    @Override
    public T next() {
        if (this.isEmpty()) {
            throw new NoSuchElementException();
        }

        @SuppressWarnings("unchecked")
        T top = (T) this.data[0];
        Iterator<T> topSource = this.sources[0];
        if (topSource.hasNext()) {
            // Current element was removed, fill next element.
            this.data[0] = topSource.next();
        } else {
            this.size--;
            // Move the last input to the top when the top input is empty.
            if (this.size > 0) {
                this.sources[0] = this.sources[this.size];
                this.data[0] = this.data[this.size];
            }
        }

        this.adjustHeap(0);

        return top;
    }

    private void constructHeap() {
        // Init data array. Skip empty iterator.
        for (int i = 0, len = this.sources.length - 1; i <= len; ) {
            if (!this.sources[i].hasNext()) {
                System.arraycopy(this.sources, i + 1,
                                 this.sources, i, len - i);
                this.size--;
                len--;
                continue;
            }
            this.data[i] = this.sources[i].next();
            i++;
        }

        // Build Heap
        for (int index = (this.size >> 1) - 1; index >= 0; index--) {
            this.adjustHeap(index);
        }
    }

    @SuppressWarnings("unchecked")
    private void adjustHeap(int parent) {
        int child;
        while ((child = (parent << 1) + 1) < this.size) {
            // Compare left and right child if right child exist.
            if (child < this.size - 1 &&
                this.compare((T) this.data[child],
                             (T) this.data[child + 1]) > 0) {
                child++;
            }
            if (this.compare((T) this.data[parent], (T) this.data[child]) > 0) {
                this.swap(parent, child);
                parent = child;
            } else {
                break;
            }
        }
    }

    private void swap(int i, int j) {
        // Swap data
        Object dataTmp = this.data[i];
        this.data[i] = this.data[j];
        this.data[j] = dataTmp;

        // Swap sources
        Iterator<T> sourceTmp = this.sources[i];
        this.sources[i] = this.sources[j];
        this.sources[j] = sourceTmp;
    }

    private boolean isEmpty() {
        return this.size <= 0;
    }
}
