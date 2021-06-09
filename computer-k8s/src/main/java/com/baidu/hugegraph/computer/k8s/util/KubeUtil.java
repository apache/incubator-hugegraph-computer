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

package com.baidu.hugegraph.computer.k8s.util;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class KubeUtil {

    /**
     * Tries a condition func until the initial delay specified.
     *
     * @param initialDelay the initial delay
     * @param interval     the check interval
     * @param timeout      the timeout period
     * @param condition    the condition
     * @return returns true if gracefully finished
     */
    public static boolean waitUntilReady(Duration initialDelay,
                                         Duration interval,
                                         Duration timeout,
                                         Supplier<Boolean> condition) {
        ScheduledExecutorService executorService =
        Executors.newSingleThreadScheduledExecutor();

        AtomicBoolean result = new AtomicBoolean(false);
        long dueDate = System.currentTimeMillis() + timeout.toMillis();
        ScheduledFuture<?> future =
                executorService.scheduleAtFixedRate(
                        () -> {
                            try {
                                result.set(condition.get());
                            } catch (Exception e) {
                                result.set(false);
                            }
                        },
                        initialDelay.toMillis(),
                        interval.toMillis(),
                        TimeUnit.MILLISECONDS);
        try {
            while (System.currentTimeMillis() < dueDate) {
                if (result.get()) {
                    future.cancel(true);
                    return true;
                }
            }
        } catch (Exception e) {
            return result.get();
        }
        future.cancel(true);
        return result.get();
    }

}
