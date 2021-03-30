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

package com.baidu.hugegraph.computer.core.common;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.master.DefaultMasterComputation;
import com.baidu.hugegraph.computer.core.master.MasterComputation;
import com.baidu.hugegraph.testutil.Assert;

public class ConfigTest {

    @Test
    public void testCreateObject() {
        Config config = ComputerContext.instance().config();
        MasterComputation masterComputation = config.createObject(
                          ComputerOptions.MASTER_COMPUTATION_CLASS);
        Assert.assertEquals(DefaultMasterComputation.class,
                            masterComputation.getClass());
    }

    @Test
    public void testCreateObjectFail() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.MASTER_COMPUTATION_CLASS,
                FakeMasterComputation.class.getName()
        );
        Config config = ComputerContext.instance().config();
        Assert.assertThrows(ComputerException.class, () -> {
            config.createObject(ComputerOptions.MASTER_COMPUTATION_CLASS);
        }, e -> {
            Assert.assertContains("Failed to create object for option",
                                  e.getMessage());
            Assert.assertContains("with modifiers \"private\"",
                                  e.getCause().getMessage());
        });
    }
}
