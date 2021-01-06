/*
 *
 *  Copyright 2017 HugeGraph Authors
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with this
 *  work for additional information regarding copyright ownership. The ASF
 *  licenses this file to You under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.baidu.hugegraph.computer.core.bsp;

import org.junit.Test;

import com.baidu.hugegraph.computer.exception.ComputerException;
import com.baidu.hugegraph.testutil.Assert;

public class LockEventTest {

    @Test
    public void testWaitMillis() {
        LockEvent lockEvent = new LockEvent();
        boolean signaled = lockEvent.waitMillis(1L);
        Assert.assertFalse(signaled);
        lockEvent.signal();
        signaled = lockEvent.waitMillis(1L);
        Assert.assertTrue(signaled);
        lockEvent.reset();
        signaled = lockEvent.waitMillis(1L);
        Assert.assertFalse(signaled);
    }

    @Test
    public void testWaitOrFail() {
        LockEvent lockEvent = new LockEvent();
        Assert.assertThrows(ComputerException.class, () -> {
            lockEvent.waitOrFail(1L);
        });
        lockEvent.signal();
        lockEvent.waitOrFail(0L);
    }
}
