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

import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.TimeUtil;
import org.slf4j.Logger;

public final class Printer {

    private static final Logger LOG = Log.logger(Printer.class);

    public static void printSummary(LoadSummary summary) {
        printCountReport(LoadReport.collect(summary));
        printMeterReport(summary);
    }

    private static void printCountReport(LoadReport report) {
        log("count metrics");
        log("vertex insert success", report.vertexInsertSuccess());
        log("vertex insert failure", report.vertexInsertFailure());
    }

    private static void printMeterReport(LoadSummary summary) {
        log("meter metrics");
        log("total time", TimeUtil.readableTime(summary.totalTime()));
        log("vertex load rate(vertices/s)", summary.loadRate());
    }

    private static void log(String message) {
        LOG.info(message);
    }

    private static void log(String key, long value) {
        LOG.info(String.format("    %-30s: %-20d", key, value));
    }

    private static void log(String key, String value) {
        LOG.info(String.format("    %-30s: %-20s", key, value));
    }
}
