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

import java.util.Objects;

/**
 * DefaultJobState is an implementation of the JobState interface.
 * It holds the state of a job including the current superstep,
 * the maximum superstep, the last superstep statistics, and the job status.
 */
public class DefaultJobState implements JobState {

    private int superstep;
    private int maxSuperstep;
    private SuperstepStat lastSuperstepStat;
    private JobStatus jobStatus;

    /**
     * Sets the current superstep.
     * @param superstep The current superstep.
     * @return The updated DefaultJobState instance.
     */
    public DefaultJobState superstep(int superstep) {
        this.superstep = superstep;
        return this;
    }

    /**
     * Sets the maximum superstep.
     * @param maxSuperstep The maximum superstep.
     * @return The updated DefaultJobState instance.
     */
    public DefaultJobState maxSuperstep(int maxSuperstep) {
        this.maxSuperstep = maxSuperstep;
        return this;
    }

    /**
     * Sets the last superstep statistics.
     * @param lastSuperstepStat The last superstep statistics.
     * @return The updated DefaultJobState instance.
     */
    public DefaultJobState lastSuperstepStat(SuperstepStat lastSuperstepStat) {
        this.lastSuperstepStat = lastSuperstepStat;
        return this;
    }

    /**
     * Sets the job status.
     * @param jobStatus The job status.
     * @return The updated DefaultJobState instance.
     */
    public DefaultJobState jobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
        return this;
    }

    /**
     * @return The current superstep.
     */
    @Override
    public int superstep() {
        return this.superstep;
    }

    /**
     * @return The maximum superstep.
     */
    @Override
    public int maxSuperstep() {
        return this.maxSuperstep;
    }

    /**
     * @return The last superstep statistics.
     */
    @Override
    public SuperstepStat lastSuperstepStat() {
        return this.lastSuperstepStat;
    }

    /**
     * @return The job status.
     */
    @Override
    public JobStatus jobStatus() {
        return this.jobStatus;
    }

    /**
     * Checks if the given object is equal to this instance.
     * @param o The object to compare with.
     * @return true if the given object is equal to this instance, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultJobState)) {
            return false;
        }

        DefaultJobState jobState = (DefaultJobState) o;
        return this.superstep == jobState.superstep &&
               this.maxSuperstep == jobState.maxSuperstep &&
               Objects.equals(this.lastSuperstepStat, jobState.lastSuperstepStat) &&
               this.jobStatus == jobState.jobStatus;
    }

    /**
     * @return The hash code of this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.superstep, this.maxSuperstep,
                            this.lastSuperstepStat, this.jobStatus);
    }

    /**
     * @return A string representation of this instance.
     */
    @Override
    public String toString() {
        return String.format("%s[superstep=%s, maxSuperStep=%s, lastSuperstepStat=%s, " +
                             "jobStatus=%s]", DefaultJobState.class.getSimpleName(),
                             superstep, maxSuperstep, lastSuperstepStat, jobStatus);
    }
}
