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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

public class TestTextId {

    @Test
    public void test() {
        TextId textId1 = new TextId();
        assertTrue(Arrays.equals(new byte[0], textId1.bytes()));
        TextId textId2 = new TextId("abc");
        assertEquals(3, textId2.length());
        TextId textId3 = new TextId("abcd");
        assertTrue(textId3.compareTo(textId2) > 0);
        assertTrue(textId2.compareTo(textId3) < 0);
        assertTrue(textId2.compareTo(textId2) == 0);
        assertNotEquals(textId2.hashCode(), textId3.hashCode());
        TextId textId4 = new TextId("abd");
        assertTrue(textId2.compareTo(textId4) < 0);
        assertTrue(textId4.compareTo(textId2) > 0);
        TextId textId5 = new TextId("abc");
        assertTrue(textId2.equals(textId5));
        assertFalse(textId2.equals(textId4));
    }

    @Test
    public void testReadWrite() throws IOException {
        TextId textId = new TextId("abc");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(bos);
        textId.write(dataOutput);
        bos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        TextId newValue = new TextId();
        newValue.read(dis);
        assertEquals("abc", newValue.toString());
        bais.close();
    }
}
