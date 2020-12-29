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

public class LongValueTest {

    @Test
    public void test() {
        LongValue longValue1 = new LongValue();
        Assert.assertEquals(ValueType.LONG, longValue1.type());
        Assert.assertEquals(0L, longValue1.value());
        LongValue longValue2 = new LongValue(Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, longValue2.value());
        longValue2.value(Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, longValue2.value());
        Assert.assertEquals(Long.hashCode(Long.MAX_VALUE),
                            longValue2.hashCode());
        Assert.assertTrue(longValue2.equals(new LongValue(longValue2.value())));
        Assert.assertFalse(longValue1.equals(longValue2));
    }

    @Test
    public void testReadWrite() throws IOException {
        LongValue longValue = new LongValue(Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, longValue.value());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(bos);
        longValue.write(dataOutput);
        bos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        LongValue newValue = new LongValue();
        newValue.read(dis);
        Assert.assertEquals(Long.MAX_VALUE, newValue.value());
        bais.close();
    }
}
