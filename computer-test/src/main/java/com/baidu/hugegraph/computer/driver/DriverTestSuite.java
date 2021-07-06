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

package com.baidu.hugegraph.computer.driver;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.computer.driver.util.JsonUtil;
import com.baidu.hugegraph.config.OptionSpace;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    ComputerOptionsTest.class,
})
public class DriverTestSuite {

    @BeforeClass
    public static void setup() {
        OptionSpace.register("computer-driver",
                             "com.baidu.hugegraph.computer.driver.config" +
                             ".ComputerOptions");
    }

    @Test
    public void jsonTest() {
        SuperstepStat superstepStat = new SuperstepStat();
        String json = JsonUtil.toJson(superstepStat);
        SuperstepStat superstepStat1 = JsonUtil.fromJson(json,
                                                         SuperstepStat.class);
        Assert.assertEquals(superstepStat, superstepStat1);
    }

    @Test
    public void testJobStatus() {
        Assert.assertFalse(JobStatus.finished(JobStatus.INITIALIZING));
        Assert.assertFalse(JobStatus.finished(JobStatus.RUNNING));
        Assert.assertTrue(JobStatus.finished(JobStatus.FAILED));
        Assert.assertTrue(JobStatus.finished(JobStatus.SUCCEEDED));
        Assert.assertTrue(JobStatus.finished(JobStatus.CANCELLED));
    }
}
