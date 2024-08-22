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

package org.apache.hugegraph.computer.k8s.operator.common;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * This class ensures the added work items are not in dirty set or currently
 * processing set, before append them to the list.
 *
 * This has been taken from official client:
 * https://github.com/kubernetes-client/java/blob/master/extended/src/main/java
 * /io/kubernetes/client/extended/workqueue/DefaultWorkQueue.java
 *
 * Which has been ported from official go client:
 * https://github.com/kubernetes/client-go/blob/master/util/workqueue/queue.go
 */
public class WorkQueue<T> {

    /**
     * queue defines the order in which we will work on items.
     * Every element of queue should be in the dirty set and not in the
     * processing set.
     */
    private final LinkedList<T> queue;

    /**
     * incoming defines all of the items that need to be processed.
     */
    private final Set<T> incoming;

    /**
     * Things that are currently being processed are in the processing set.
     * These things may be simultaneously in the dirty set. When we finish
     * processing something and remove it from this set, we'll check if
     * it's in the dirty set, and if so, add it to the queue.
     */
    private final Set<T> processing;

    private boolean shutdown;

    public WorkQueue() {
        this.queue = new LinkedList<>();
        this.incoming = new HashSet<>();
        this.processing = new HashSet<>();
    }

    public synchronized void add(T item) {
        if (this.shutdown) {
            return;
        }

        if (this.incoming.contains(item)) {
            return;
        }

        this.incoming.add(item);
        if (this.processing.contains(item)) {
            return;
        }

        this.queue.add(item);
        this.notify();
    }

    public synchronized int length() {
        return this.queue.size();
    }

    public synchronized T get() throws InterruptedException {
        while (this.queue.size() == 0) {
            if (this.shutdown) {
                // We must be shutting down
                return null;
            } else {
                this.wait();
            }
        }
        T obj = this.queue.poll();
        this.processing.add(obj);
        this.incoming.remove(obj);
        return obj;
    }

    public synchronized void done(T item) {
        this.processing.remove(item);
        if (this.incoming.contains(item)) {
            this.queue.add(item);
            this.notify();
        }
    }

    public synchronized void shutdown() {
        this.shutdown = true;
        this.notifyAll();
    }

    public synchronized boolean isShutdown() {
        return this.shutdown;
    }
}
