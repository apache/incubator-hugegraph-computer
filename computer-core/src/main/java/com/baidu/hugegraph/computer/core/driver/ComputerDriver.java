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

package com.baidu.hugegraph.computer.core.driver;

import java.io.InputStream;
import java.util.Map;

public interface ComputerDriver {

    /**
     * This method is called when create algorithm, the user will upload a jar.
     * @param input The input from which to create a jar.
     * @param algorithmName The algorithm name is unique. If the jar
     *                      correspond to the algorithm name is exist, it
     *                      will overwrite the previous jar.
     */
    void uploadAlgorithmJar(InputStream input, String algorithmName);

    /**
     * Submit the algorithm to the cluster, the parameters are in config.
     * @return the job id represents the job submitted.
     */
    String submitJob(String algorithmName, Map<String, String> params);

    /**
     * Cancel the job with specified jobId, the parameters are in config.
     * throw ComputerException if the job can't cancelled or can't found
     * the job.
     */
    void cancelJob(String jobId, Map<String, String> params);

    /**
     * Trace the job, it create a separate thread to trace the job and return
     * immediately. Throws ComputerException if the job is tracing by another
     * thread.
     */
    void traceJob(String jobId, Map<String, String> params);

    /**
     * Get the main reason the job is failed. If the job is running or can't
     * found the job throws ComputerException. If the job is exited
     * successfully, return null.
     */
    String diagnostics(String jobId, Map<String, String> params);

    /**
     * Get the log of specified task. Throw ComputerException if the job
     * can't found.
     * @param jobId The job id to which the job belongs.
     * @param task 0 for master, the task id from 1
     * @param offset The offset the log read from, must be >=0
     * @param length The length the log read, -1 for not limited.
     * @param params The parameters needed to read log are in config
     * @return The log content.
     */
    String log(String jobId, int task, long offset, long length,
               Map<String, String> params);
}
