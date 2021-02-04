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

package com.baidu.hugegraph.computer.core.graph.id;

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.testutil.Assert;

public class Utf8IdTest extends UnitTestBase {

    @Test
    public void test() {
        Utf8Id utf8Id1 = new Utf8Id();
        Utf8Id utf8Id2 = new Utf8Id("abc");
        Utf8Id utf8Id3 = new Utf8Id("abcd");
        Utf8Id utf8Id4 = new Utf8Id("abd");
        Utf8Id utf8Id5 = new Utf8Id("abc");
        Utf8Id utf8Id6 = new Utf8Id("100");

        Assert.assertEquals(IdType.UTF8, utf8Id1.type());
        Assert.assertArrayEquals(new byte[0], utf8Id1.bytes());
        Assert.assertEquals(3, utf8Id2.length());

        Assert.assertEquals(IdType.UTF8, utf8Id2.type());
        IdValue idValue = utf8Id2.idValue();
        Assert.assertEquals(ValueType.ID_VALUE, idValue.type());
        Assert.assertEquals(utf8Id2, idValue.id());

        Assert.assertEquals("abc", utf8Id2.asObject());
        Assert.assertThrows(NumberFormatException.class, () -> {
            utf8Id2.asLong();
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("For input string"));
        });

        Assert.assertEquals("100", utf8Id6.asObject());
        Assert.assertEquals(100L, utf8Id6.asLong());
        Assert.assertArrayEquals(CoderUtil.encode("100").array(),
                                 utf8Id6.asBytes());

        Assert.assertTrue(utf8Id3.compareTo(utf8Id2) > 0);
        Assert.assertTrue(utf8Id2.compareTo(utf8Id3) < 0);
        Assert.assertTrue(utf8Id2.compareTo(utf8Id2) == 0);
        Assert.assertTrue(utf8Id2.compareTo(utf8Id4) < 0);
        Assert.assertTrue(utf8Id4.compareTo(utf8Id2) > 0);

        Assert.assertEquals(utf8Id2, utf8Id5);
        Assert.assertNotEquals(utf8Id2, utf8Id4);

        Assert.assertEquals(utf8Id2.hashCode(), utf8Id5.hashCode());
        Assert.assertNotEquals(utf8Id2.hashCode(), utf8Id3.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertIdEqualAfterWriteAndRead(new Utf8Id("abc"));
    }
}
