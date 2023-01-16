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

package org.apache.hugegraph.computer.core.output.hg.metrics;

import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.time.StopWatch;

public final class LoadSummary {

    private final LongAdder vertexLoaded;
    private final StopWatch totalTimer;
    private final LoadMetrics metrics;

    public LoadSummary() {
        this.vertexLoaded = new LongAdder();
        this.totalTimer = new StopWatch();
        this.metrics = new LoadMetrics();
    }

    public LoadMetrics metrics() {
        return this.metrics;
    }

    public long vertexLoaded() {
        return this.vertexLoaded.longValue();
    }

    public void plusLoaded(int count) {
        this.vertexLoaded.add(count);
    }

    public long totalTime() {
        return this.totalTimer.getTime();
    }

    public void startTimer() {
        if (!this.totalTimer.isStarted()) {
            this.totalTimer.start();
        }
    }

    public void stopTimer() {
        if (!this.totalTimer.isStopped()) {
            this.totalTimer.stop();
        }
    }

    public long loadRate() {
        long totalTime = this.totalTime();
        if (totalTime == 0) {
            return -1;
        }
        return this.vertexLoaded() * 1000 / totalTime;
    }
}
