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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class LoserTreeInputsSorting<T> extends AbstractInputsSorting<T> {

    private static final Object INFINITY_LEAF = new Object();

    private final Object[] leaves;
    private final int size;
    private final int[] tree;

    public LoserTreeInputsSorting(Collection<? extends Iterator<T>> sources) {
        this(sources, null);
    }

    public LoserTreeInputsSorting(Collection<? extends Iterator<T>> sources,
                                  Comparator<? super T> comparator) {
        super(sources, comparator);

        this.size = sources.size();
        this.leaves = new Object[this.size];
        this.tree = new int[this.size];

        this.constructLoserTree();
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

        int winnerIndex = this.tree[0];

        @SuppressWarnings("unchecked")
        T winner = (T) this.leaves[winnerIndex];

        this.fill(winnerIndex);
        this.adjust(winnerIndex);

        return winner;
    }

    private boolean isEmpty() {
        return this.leaves[this.tree[0]] == INFINITY_LEAF;
    }

    private void constructLoserTree() {
        // Init leaves
        for (int i = 0; i < this.sources.length; i++) {
            if (this.sources[i].hasNext()) {
                this.leaves[i] = this.sources[i].next();
            } else {
                this.leaves[i] = INFINITY_LEAF;
            }
        }

        // Init tree to winner
        int winner = 0;
        for (int i = 0; i < this.size; i++) {
            if (this.beat(i, winner)) {
                winner = i;
            }
        }
        Arrays.fill(this.tree, winner);

        // Init loser tree
        for (int i = this.size - 1; i >= 0; i--) {
            this.adjust(i);
        }
    }

    private void fill(int index) {
        Iterator<T> source = this.sources[index];
        if (source.hasNext()) {
            this.leaves[index] = source.next();
        } else {
            this.leaves[index] = INFINITY_LEAF;
        }
    }

    private void adjust(int index) {
        int t = (this.size + index) >> 1;
        while (t > 0) {
            // Save loser
            if (this.beat(this.tree[t], index)) {
                int temp = this.tree[t];
                this.tree[t] = index;
                index = temp;
            }
            t = t >> 1;
        }
        this.tree[0] = index;
    }

    /**
     * Judge whether index1 can beat index2
     * if order by asc. smaller one is winner. bigger one is loser.
     */
    private boolean beat(int index1, int index2) {
        @SuppressWarnings("unchecked")
        T t1 = (T) this.leaves[index1];
        @SuppressWarnings("unchecked")
        T t2 = (T) this.leaves[index2];

        if (t1 == INFINITY_LEAF) {
            return false;
        }
        if (t2 == INFINITY_LEAF) {
            return true;
        }

        int result = this.compare(t1, t2);
        if (result == 0) {
            return index1 > index2;
        }
        return result < 0;
    }
}
