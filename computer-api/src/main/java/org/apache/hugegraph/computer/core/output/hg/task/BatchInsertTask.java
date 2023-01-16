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

package org.apache.hugegraph.computer.core.output.hg.task;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.output.hg.metrics.LoadSummary;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.exception.ServerException;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class BatchInsertTask extends InsertTask {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public BatchInsertTask(Config config, HugeClient client,
                           List<Vertex> batch, LoadSummary loadSummary) {
        super(config, client, batch, loadSummary);
    }

    @Override
    public void run() {
        int retryCount = 0;
        int retryTimes = this.config.get(ComputerOptions.OUTPUT_RETRY_TIMES);
        do {
            try {
                this.insertBatch(this.batch);
                break;
            } catch (ClientException e) {
                LOG.debug("client exception: {}", e.getMessage());
                Throwable cause = e.getCause();
                if (cause != null && cause.getMessage() != null) {
                    if (StringUtils.containsAny(cause.getMessage(),
                                                UNACCEPTABLE_MESSAGES)) {
                        throw e;
                    }
                }
                retryCount = this.waitThenRetry(retryCount, e);
            } catch (ServerException e) {
                String message = e.getMessage();
                LOG.error("server exception: {}", message);
                if (UNACCEPTABLE_EXCEPTIONS.contains(e.exception())) {
                    throw e;
                }
                if (StringUtils.containsAny(message, UNACCEPTABLE_MESSAGES)) {
                    throw e;
                }
                retryCount = this.waitThenRetry(retryCount, e);
            }
        } while (retryCount > 0 && retryCount <= retryTimes);

        int count = this.batch.size();
        // This metrics just for current element mapping
        this.plusLoadSuccess(count);
    }

    private int waitThenRetry(int retryCount, RuntimeException e) {
        int retryTimes = this.config.get(ComputerOptions.OUTPUT_RETRY_TIMES);
        if (retryTimes <= 0) {
            return retryCount;
        }

        if (++retryCount > retryTimes) {
            LOG.error("Batch insert has been retried more than {} times",
                      retryTimes);
            throw e;
        }

        long interval = (1L << retryCount) *
                        this.config.get(ComputerOptions.OUTPUT_RETRY_INTERVAL);
        LOG.debug("Batch insert will sleep {} seconds then do the {}th retry",
                  interval, retryCount);
        try {
            Thread.sleep(interval * 1000L);
        } catch (InterruptedException ignored) {
            // That's fine, just continue.
        }
        return retryCount;
    }
}
