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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

public class DoubleValueTest {

    @Test
    public void test() {
        DoubleValue doubleValue1 = new DoubleValue();
        Assert.assertEquals(ValueType.DOUBLE, doubleValue1.type());
        Assert.assertTrue(0.0D == doubleValue1.value());
        DoubleValue doubleValue2 = new DoubleValue(Double.MIN_VALUE);
        Assert.assertTrue(Double.MIN_VALUE == doubleValue2.value());
        doubleValue2.value(Double.MAX_VALUE);
        Assert.assertTrue(Double.MAX_VALUE == doubleValue2.value());
        Assert.assertEquals(Double.hashCode(Double.MAX_VALUE),
                            doubleValue2.hashCode());
        Assert.assertEquals(doubleValue2,
                            new DoubleValue(doubleValue2.value()));
        Assert.assertFalse(doubleValue1.equals(doubleValue2));
    }

    @Test
    public void testReadWrite() throws IOException {
        DoubleValue doubleValue = new DoubleValue(Double.MAX_VALUE);
        Assert.assertTrue(Double.MAX_VALUE == doubleValue.value());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(bos);
        doubleValue.write(dataOutput);
        bos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        DoubleValue newValue = new DoubleValue();
        newValue.read(dis);
        Assert.assertTrue(Double.MAX_VALUE == newValue.value());
        bais.close();
    }
}
