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

package org.apache.hugegraph.computer.core.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public final class Consumers<V> {

    private static final int CPU_CORE_NUM = 4;
    public static final int THREADS = 4 + CPU_CORE_NUM / 4;
    public static final int QUEUE_WORKER_SIZE = 1000;
    public static final long CONSUMER_WAKE_PERIOD = 1;

    private static final Logger LOG = Log.logger(Consumers.class);

    private final ExecutorService executor;
    private final Consumer<V> consumer;
    private final Runnable done;

    private final int workers;
    private final int queueSize;
    private final CountDownLatch latch;
    private final BlockingQueue<V> queue;

    private volatile boolean ending = false;
    private volatile Throwable exception = null;

    public Consumers(ExecutorService executor, Consumer<V> consumer) {
        this(executor, consumer, null);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer, Runnable done) {
        this.executor = executor;
        this.consumer = consumer;
        this.done = done;

        int workers = THREADS;
        if (this.executor instanceof ThreadPoolExecutor) {
            workers = ((ThreadPoolExecutor) this.executor).getCorePoolSize();
        }
        this.workers = workers;
        this.queueSize = QUEUE_WORKER_SIZE * workers;
        this.latch = new CountDownLatch(workers);
        this.queue = new ArrayBlockingQueue<>(this.queueSize);
    }

    public void start(String name) {
        this.ending = false;
        this.exception = null;
        if (this.executor == null) {
            return;
        }
        LOG.info("Starting {} workers[{}] with queue size {}...",
                 this.workers, name, this.queueSize);
        for (int i = 0; i < this.workers; i++) {
            this.executor.execute(this::runAndDone);
        }
    }

    private void runAndDone() {
        try {
            this.run();
        } catch (Throwable e) {
            // Only the first exception of one thread can be stored
            this.exception = e;
            if (!(e instanceof StopExecution)) {
                LOG.error("Error when running task", e);
            }
        } finally {
            this.done();
            this.latch.countDown();
        }
    }

    private void run() {
        LOG.debug("Start to work...");
        while (!this.ending) {
            this.consume();
        }
        assert this.ending;
        while (this.consume()) {

        }

        LOG.debug("Worker finished");
    }

    private boolean consume() {
        V elem;
        try {
            elem = this.queue.poll(CONSUMER_WAKE_PERIOD, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
            return true;
        }
        if (elem == null) {
            return false;
        }
        // do job
        this.consumer.accept(elem);
        return true;
    }

    private void done() {
        if (this.done == null) {
            return;
        }

        try {
            this.done.run();
        } catch (Throwable e) {
            if (this.exception == null) {
                this.exception = e;
            } else {
                LOG.warn("Error while calling done()", e);
            }
        }
    }

    private Throwable throwException() {
        assert this.exception != null;
        Throwable e = this.exception;
        this.exception = null;
        return e;
    }

    public void provide(V v) throws Throwable {
        if (this.executor == null) {
            assert this.exception == null;
            // do job directly if without thread pool
            this.consumer.accept(v);
        } else if (this.exception != null) {
            throw this.throwException();
        } else {
            try {
                this.queue.put(v);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while enqueue", e);
            }
        }
    }

    public void await() throws Throwable {
        this.ending = true;
        if (this.executor == null) {
            // call done() directly if without thread pool
            this.done();
        } else {
            try {
                this.latch.await();
            } catch (InterruptedException e) {
                String error = "Interrupted while waiting for consumers";
                this.exception = new ComputerException(error, e);
                LOG.warn(error, e);
            }
        }

        if (this.exception != null) {
            throw this.throwException();
        }
    }

    public ExecutorService executor() {
        return this.executor;
    }

    public static RuntimeException wrapException(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new ComputerException("Error when running task: %s",
                                    ComputerException.rootCause(e).getMessage(),
                                    e);
    }

    public static class StopExecution extends ComputerException {

        private static final long serialVersionUID = -371829356182454517L;

        public StopExecution(String message) {
            super(message);
        }

        public StopExecution(String message, Object... args) {
            super(message, args);
        }
    }
}
