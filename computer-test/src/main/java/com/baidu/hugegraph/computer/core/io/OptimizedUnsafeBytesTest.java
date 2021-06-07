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

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.testutil.Assert;

@SuppressWarnings("resource")
public class OptimizedUnsafeBytesTest {

    @Test
    public void testConstructor() {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.SMALL_BUF_SIZE);
        Assert.assertEquals(0, output.position());

        OptimizedBytesOutput output2 = new OptimizedBytesOutput(16);
        Assert.assertEquals(0, output2.position());

        OptimizedBytesInput input = new OptimizedBytesInput(output.buffer());
        Assert.assertEquals(0, input.position());

        OptimizedBytesInput input2 = new OptimizedBytesInput(output.buffer(),
                                                             4);
        Assert.assertEquals(0, input2.position());
    }

    @Test
    public void testDuplicate() throws IOException {
        OptimizedBytesInput raw = inputByString("apple");
        OptimizedBytesInput dup = raw.duplicate();
        raw.readByte();
        Assert.assertEquals(1, raw.position());
        Assert.assertEquals(0, dup.position());
    }

    private static OptimizedBytesInput inputByString(String s)
                                                     throws IOException {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.SMALL_BUF_SIZE);
        output.writeBytes(s);
        return new OptimizedBytesInput(output.toByteArray());
    }
}
