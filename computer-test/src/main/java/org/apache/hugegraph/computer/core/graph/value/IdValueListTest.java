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

package org.apache.hugegraph.computer.core.graph.value;

import java.io.IOException;

import org.apache.commons.collections.ListUtils;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IdValueListTest extends UnitTestBase {

    @Test
    public void test() {
        Id longId1 = BytesId.of(100L);
        Id longId2 = BytesId.of(200L);
        IdList listValue1 = new IdList();
        IdList listValue2 = new IdList();

        listValue1.add(longId1);
        listValue2.add(longId1);

        Assert.assertEquals(ValueType.ID_LIST, listValue1.valueType());
        Assert.assertEquals(ValueType.ID, listValue1.elemType());
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1),
                          listValue1.values()));
        Assert.assertEquals(listValue1, listValue2);

        listValue2.add(longId2);
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1, longId2),
                          listValue2.values()));
        Assert.assertNotEquals(listValue1, listValue2);
        Assert.assertEquals(ListUtils.hashCodeForList(
                            Lists.newArrayList(longId1)),
                            listValue1.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        Id longId1 = BytesId.of(100L);
        Id longId2 = BytesId.of(200L);
        IdList oldValue = new IdList();
        oldValue.add(longId1);
        oldValue.add(longId2);
        assertValueEqualAfterWriteAndRead(oldValue);
    }

    @Test
    public void testCompare() {
        Id longId1 = BytesId.of(100L);
        Id longId2 = BytesId.of(200L);
        IdList value1 = new IdList();
        value1.add(longId1);
        IdList value2 = new IdList();
        value2.add(longId1);
        IdList value3 = new IdList();
        value3.add(longId1);
        value3.add(longId2);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }
}
