/*
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

package org.apache.hugegraph.computer.core.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class SerializeUtilTest {

    @Test
    public void testConvertBytesAndObject() {
        Id id1 = BytesId.of(1L);
        byte[] bytes = SerializeUtil.toBytes(id1);
        Id id2 = BytesId.of(2L);
        SerializeUtil.fromBytes(bytes, id2);

        Assert.assertEquals(id1, id2);
    }

    @Test
    public void testConvertBytesAndListObject() {
        Id id1 = BytesId.of(1L);
        Id id2 = BytesId.of(2L);
        List<Id> list1 = new ArrayList<>(2);
        list1.add(id1);
        list1.add(id2);

        byte[] bytes = SerializeUtil.toBytes(list1);
        List<Id> list2 = SerializeUtil.fromBytes(bytes, () -> new BytesId());

        Assert.assertEquals(list1, list2);
    }
}
