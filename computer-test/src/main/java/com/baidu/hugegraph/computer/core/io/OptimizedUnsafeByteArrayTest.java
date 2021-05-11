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

package com.baidu.hugegraph.computer.core.io;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

@SuppressWarnings("resource")
public class OptimizedUnsafeByteArrayTest {

    @Test
    public void testConstructor() {
        UnsafeBytesOutput output = new OptimizedUnsafeBytesOutput();
        Assert.assertEquals(0, output.position());

        UnsafeBytesOutput output2 = new OptimizedUnsafeBytesOutput(16);
        Assert.assertEquals(0, output2.position());

        UnsafeBytesInput input = new OptimizedUnsafeBytesInput(output.buffer());
        Assert.assertEquals(0, input.position());

        UnsafeBytesInput input2 = new OptimizedUnsafeBytesInput(output.buffer(),
                                                                4L);
        Assert.assertEquals(0, input2.position());
    }
}
