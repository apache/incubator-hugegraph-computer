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

package org.apache.hugegraph.computer.driver;

import java.io.Closeable;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ComputerDriver extends Closeable {

    /**
     * This method is called when create algorithm, the user will upload a jar.
     * @param algorithmName The algorithm name is unique. If the jar
     *                      correspond to the algorithm name is exist, it
     *                      will overwrite the previous jar.
     * @param input The input stream from which to create a jar.
     */
    void uploadAlgorithmJar(String algorithmName, InputStream input);

    /**
     * Submit the algorithm to the cluster, the parameters are in config.
     * @return the job id represents the job submitted.
     */
    String submitJob(String algorithmName, Map<String, String> params);

    /**
     * Cancel the job with specified jobId, the parameters are in config.
     * Throws ComputerException if the job can't cancelled or can't found
     * the job.
     * @param params reserved for other parameters in addition to jobId used
     *               to cancel job.
     * @return Whether the job was successfully cancelled
     */
    boolean cancelJob(String jobId, Map<String, String> params);

    /**
     * Wait the job to finish, it will trace the execution of job and notify
     * the observer at every superstep. Throws ComputerException if the
     * job is waiting by another thread.
     * @param params reserved for other parameters in addition to jobId used
     *               to wait job.
     * @return future for watch the job
     */
    CompletableFuture<Void> waitJobAsync(String jobId,
                                         Map<String, String> params,
                                         JobObserver observer);

    /**
     * Get the current job state. Throws ComputerException if can't found the
     * job.
     * @param params reserved for other parameters in addition to jobId used
     *               to get job state.
     */
    JobState jobState(String jobId, Map<String, String> params);

    /**
     * Get the superstep stats of the job. Throws ComputerException if can't
     * found the job.
     * @param params reserved for other parameters in addition to jobId used
     *               to get superstep stats.
     */
    List<SuperstepStat> superstepStats(String jobId,
                                       Map<String, String> params);

    /**
     * Get the main reason the job is failed. Throws ComputerException if the
     * job is running or can't found the job. Return null if the job is exited
     * successfully.
     * @param params reserved for other parameters in addition to jobId used
     *               to get diagnostics.
     */
    String diagnostics(String jobId, Map<String, String> params);

    /**
     * Get the log of specified container. Throws ComputerException if the job
     * can't found.
     * @param jobId The job id to which the job belongs.
     * @param containerId 0 for master, the worker's container id goes from 1
     *                    to the number of workers.
     * @param offset The offset the log read from, must be >=0
     * @param length The length the log read, -1 for not limited.
     * @param params The parameters needed to read log are in config
     * @return The log content.
     */
    String log(String jobId, int containerId, long offset, long length,
               Map<String, String> params);
}
