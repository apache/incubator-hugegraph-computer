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

package com.baidu.hugegraph.computer.k8s;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.k8s.operator.common.OperatorResult;
import com.baidu.hugegraph.computer.k8s.operator.common.Request;

public class OperatorTest {

    @Test
    public void testResult() {
        OperatorResult result = new OperatorResult(true);
        OperatorResult result2 = new OperatorResult(false);
        Assert.assertNotEquals(result, result2);
        Assert.assertNotEquals(null, result);

        OperatorResult result3 = new OperatorResult(true);
        Assert.assertEquals(result, result3);
        Assert.assertEquals(result.hashCode(), result3.hashCode());
    }

    @Test
    public void testRequest() {
        Request request = new Request("testA");
        Request request2 = new Request("testB");
        Assert.assertNotEquals(request, request2);
        Assert.assertNotEquals(null, request);

        Request request3 = new Request("testA");
        Assert.assertEquals(request, request3);
        Assert.assertEquals(request.hashCode(), request3.hashCode());

        Assert.assertEquals(1, request.retryIncrGet());
        Request request4 = request.namespace("namespace-test")
                                  .name("name-test");
        Assert.assertEquals("namespace-test", request4.namespace());
        Assert.assertEquals("name-test", request4.name());
    }
}
