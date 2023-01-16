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

public class DefaultJobState implements JobState {

    private int superstep;
    private int maxSuperstep;
    private SuperstepStat lastSuperstepStat;
    private JobStatus jobStatus;

    public DefaultJobState superstep(int superstep) {
        this.superstep = superstep;
        return this;
    }

    public DefaultJobState maxSuperstep(int maxSuperstep) {
        this.maxSuperstep = maxSuperstep;
        return this;
    }

    public DefaultJobState lastSuperstepStat(SuperstepStat lastSuperstepStat) {
        this.lastSuperstepStat = lastSuperstepStat;
        return this;
    }

    public DefaultJobState jobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
        return this;
    }

    @Override
    public int superstep() {
        return this.superstep;
    }

    @Override
    public int maxSuperstep() {
        return this.maxSuperstep;
    }

    @Override
    public SuperstepStat lastSuperstepStat() {
        return this.lastSuperstepStat;
    }

    @Override
    public JobStatus jobStatus() {
        return this.jobStatus;
    }

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
               Objects.equals(this.lastSuperstepStat,
                              jobState.lastSuperstepStat) &&
               this.jobStatus == jobState.jobStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.superstep, this.maxSuperstep,
                            this.lastSuperstepStat, this.jobStatus);
    }
}
