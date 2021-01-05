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
import java.util.Arrays;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.testutil.Assert;

public class Utf8IdTest {

    @Test
    public void test() {
        Utf8Id utf8Id1 = new Utf8Id();
        Assert.assertEquals(ValueType.UTF8_ID, utf8Id1.type());
        Assert.assertTrue(Arrays.equals(new byte[0], utf8Id1.bytes()));
        Utf8Id utf8Id2 = new Utf8Id("abc");
        Assert.assertEquals(3, utf8Id2.length());
        Utf8Id utf8Id3 = new Utf8Id("abcd");
        Assert.assertTrue(utf8Id3.compareTo(utf8Id2) > 0);
        Assert.assertTrue(utf8Id2.compareTo(utf8Id3) < 0);
        Assert.assertTrue(utf8Id2.compareTo(utf8Id2) == 0);
        Assert.assertNotEquals(utf8Id2.hashCode(), utf8Id3.hashCode());
        Utf8Id utf8Id4 = new Utf8Id("abd");
        Assert.assertTrue(utf8Id2.compareTo(utf8Id4) < 0);
        Assert.assertTrue(utf8Id4.compareTo(utf8Id2) > 0);
        Utf8Id utf8Id5 = new Utf8Id("abc");
        Assert.assertTrue(utf8Id2.equals(utf8Id5));
        Assert.assertFalse(utf8Id2.equals(utf8Id4));
    }

    @Test
    public void testReadWrite() throws IOException {
        Utf8Id utf8Id = new Utf8Id("abc");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(bos);
        utf8Id.write(dataOutput);
        bos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        Utf8Id newValue = new Utf8Id();
        newValue.read(dis);
        Assert.assertEquals("abc", newValue.toString());
        bais.close();
    }
}
