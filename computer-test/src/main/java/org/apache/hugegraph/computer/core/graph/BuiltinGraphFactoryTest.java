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

package org.apache.hugegraph.computer.core.graph;

import java.util.UUID;

import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class BuiltinGraphFactoryTest extends UnitTestBase {

    @Test
    public void testCreateLongId() {
        long value = 1L;
        GraphFactory graphFactory = graphFactory();
        Id id = graphFactory.createId(value);
        Assert.assertEquals(IdType.LONG, id.idType());
        Assert.assertEquals(BytesId.of(value), id);
    }

    @Test
    public void testCreateUtf8Id() {
        String value = "John";
        GraphFactory graphFactory = graphFactory();
        Id id = graphFactory.createId(value);
        Assert.assertEquals(IdType.UTF8, id.idType());
        Assert.assertEquals(BytesId.of(value), id);
    }

    @Test
    public void testCreateUuidId() {
        UUID uuid = UUID.randomUUID();
        GraphFactory graphFactory = graphFactory();
        Id id = graphFactory.createId(uuid);
        Assert.assertEquals(IdType.UUID, id.idType());
        Assert.assertEquals(BytesId.of(uuid), id);
    }

    @Test
    public void testCreateValue() {
        GraphFactory factory = context().graphFactory();
        Assert.assertEquals(ValueType.NULL,
                            factory.createValue(ValueType.NULL.code())
                                   .valueType());
        Assert.assertEquals(ValueType.LONG,
                            factory.createValue(ValueType.LONG.code())
                                   .valueType());
        Assert.assertEquals(ValueType.DOUBLE,
                            factory.createValue(ValueType.DOUBLE.code())
                                   .valueType());
        Assert.assertEquals(ValueType.ID,
                            factory.createValue(ValueType.ID.code())
                                   .valueType());
        Assert.assertEquals(ValueType.ID_LIST,
                            factory.createValue(ValueType.ID_LIST.code())
                                   .valueType());
        Assert.assertEquals(ValueType.ID_LIST_LIST,
                            factory.createValue(
                                    ValueType.ID_LIST_LIST.code())
                                   .valueType());

        Assert.assertEquals(ValueType.NULL,
                            factory.createValue(ValueType.NULL).valueType());
        Assert.assertEquals(ValueType.LONG,
                            factory.createValue(ValueType.LONG).valueType());
        Assert.assertEquals(ValueType.DOUBLE,
                            factory.createValue(ValueType.DOUBLE).valueType());
        Assert.assertEquals(ValueType.ID,
                            factory.createValue(ValueType.ID)
                                   .valueType());
        Assert.assertEquals(ValueType.ID_LIST,
                            factory.createValue(ValueType.ID_LIST)
                                   .valueType());
        Assert.assertEquals(ValueType.ID_LIST_LIST,
                            factory.createValue(ValueType.ID_LIST_LIST)
                                   .valueType());

        Assert.assertThrows(NullPointerException.class, () -> {
            factory.createValue(null);
        });
    }
}
