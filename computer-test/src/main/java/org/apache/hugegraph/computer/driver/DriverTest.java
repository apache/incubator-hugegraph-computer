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

import org.apache.hugegraph.computer.driver.util.JsonUtil;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class DriverTest {

    @Test
    public void jsonUtilTest() {
        SuperstepStat superstepStat = new SuperstepStat();
        String json = JsonUtil.toJson(superstepStat);
        SuperstepStat superstepStat1 = JsonUtil.fromJson(json,
                                                         SuperstepStat.class);
        Assert.assertEquals(superstepStat, superstepStat1);

        Assert.assertThrows(RuntimeException.class, () -> {
            JsonUtil.fromJson("123", SuperstepStat.class);
        });
    }

    @Test
    public void testJobStatus() {
        Assert.assertFalse(JobStatus.finished(JobStatus.INITIALIZING));
        Assert.assertFalse(JobStatus.finished(JobStatus.RUNNING));
        Assert.assertTrue(JobStatus.finished(JobStatus.FAILED));
        Assert.assertTrue(JobStatus.finished(JobStatus.SUCCEEDED));
        Assert.assertTrue(JobStatus.finished(JobStatus.CANCELLED));
    }

    @Test
    public void testJobStateAndSuperstepStat() {
        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        jobState.superstep(3);
        jobState.maxSuperstep(99);
        SuperstepStat superstepStat = new SuperstepStat();
        superstepStat.vertexCount(1);
        superstepStat.edgeCount(1);
        superstepStat.finishedVertexCount(1);
        superstepStat.messageCount(1);
        superstepStat.active(true);
        superstepStat.messageBytes(1);
        jobState.lastSuperstepStat(superstepStat);

        Assert.assertEquals(JobStatus.INITIALIZING, jobState.jobStatus());
        Assert.assertEquals(3, jobState.superstep());
        Assert.assertEquals(99, jobState.maxSuperstep());

        Assert.assertEquals(1, superstepStat.vertexCount());
        Assert.assertEquals(1, superstepStat.edgeCount());
        Assert.assertEquals(1, superstepStat.finishedVertexCount());
        Assert.assertEquals(1, superstepStat.messageCount());
        Assert.assertTrue(superstepStat.active());
        Assert.assertEquals(1, superstepStat.messageBytes());

        SuperstepStat superstepStat2 = new SuperstepStat();
        superstepStat2.vertexCount(1);
        superstepStat2.edgeCount(1);
        superstepStat2.finishedVertexCount(1);
        superstepStat2.messageCount(1);
        superstepStat2.active(true);
        superstepStat2.messageBytes(1);
        Assert.assertEquals(superstepStat, superstepStat2);
        Assert.assertEquals(superstepStat.hashCode(),
                            superstepStat2.hashCode());
    }
}
