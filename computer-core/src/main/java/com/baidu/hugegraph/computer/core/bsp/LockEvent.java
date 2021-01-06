/*
 *
 *  Copyright 2017 HugeGraph Authors
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with this
 *  work for additional information regarding copyright ownership. The ASF
 *  licenses this file to You under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.baidu.hugegraph.computer.core.bsp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.baidu.hugegraph.computer.exception.ComputerException;
import com.baidu.hugegraph.util.E;

public class LockEvent implements BspEvent {

    private Lock lock = new ReentrantLock();
    private Condition cond = lock.newCondition();
    private boolean signaled = false;

    @Override
    public void reset() {
        this.lock.lock();
        try {
            this.signaled = false;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void signal() {
        this.lock.lock();
        try {
            this.signaled = true;
            this.cond.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean waitMillis(long time) {
        E.checkArgument(time >= 0L, "The time must be >= 0, but got '%d'.",
                        time);
        long deadline = System.currentTimeMillis() + time;
        this.lock.lock();
        try {
            while (!this.signaled) {
                time = deadline - System.currentTimeMillis();
                this.cond.await(time, TimeUnit.MILLISECONDS);
                if (System.currentTimeMillis() > deadline) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw new ComputerException("Interrupted while waiting '%d' ms.",
                                        e, time);
        } finally {
            this.lock.unlock();
        }
        return true;
    }

    @Override
    public void waitOrFail(long time) {
        E.checkArgument(time >= 0L, "The time must be >= 0, but got '%d'.",
                        time);
        long deadline = System.currentTimeMillis() + time;
        while (!this.waitMillis(time)) {
            if (System.currentTimeMillis() > deadline) {
                throw new ComputerException("Timeout waiting, time='%d' ms.",
                                            time);
            }
        }
    }
}
