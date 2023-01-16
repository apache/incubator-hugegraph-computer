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

package org.apache.hugegraph.computer.core.input.hg;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.ElementFetcher;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Shard;

public abstract class HugeElementFetcher<T> implements ElementFetcher<T> {

    private final Config config;
    private final HugeClient client;
    private Iterator<T> localBatch;
    private T next;

    public HugeElementFetcher(Config config, HugeClient client) {
        this.config = config;
        this.client = client;
    }

    protected Config config() {
        return this.config;
    }

    protected HugeClient client() {
        return this.client;
    }

    protected int pageSize() {
        return this.config.get(ComputerOptions.INPUT_SPLIT_PAGE_SIZE);
    }

    @Override
    public void prepareLoadInputSplit(InputSplit split) {
        this.localBatch = this.fetch(split);
    }

    @Override
    public boolean hasNext() {
        if (this.next != null) {
            return true;
        }
        if (this.localBatch != null && this.localBatch.hasNext()) {
            this.next = this.localBatch.next();
            return true;
        } else {
            this.localBatch = null;
            return false;
        }
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        T current = this.next;
        this.next = null;
        return current;
    }

    public abstract Iterator<T> fetch(InputSplit split);

    public static Shard toShard(InputSplit split) {
        return new Shard(split.start(), split.end(), 0);
    }
}
