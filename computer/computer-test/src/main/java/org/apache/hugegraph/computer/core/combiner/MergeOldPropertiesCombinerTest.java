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

package org.apache.hugegraph.computer.core.combiner;

import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class MergeOldPropertiesCombinerTest extends UnitTestBase {

    @Test
    public void testCombine() {
        Properties properties1 = graphFactory().createProperties();
        properties1.put("name", BytesId.of("marko"));
        properties1.put("city", BytesId.of("Beijing"));

        Properties properties2 = graphFactory().createProperties();
        properties2.put("name", BytesId.of("josh"));
        properties2.put("age", BytesId.of("18"));

        Properties expect = graphFactory().createProperties();
        expect.put("name", BytesId.of("josh"));
        expect.put("age", BytesId.of("18"));
        expect.put("city", BytesId.of("Beijing"));

        Properties properties = graphFactory().createProperties();
        PropertiesCombiner combiner = new MergeOldPropertiesCombiner();
        combiner.combine(properties1, properties2, properties);
        Assert.assertEquals(expect, properties);
    }

    @Test
    public void testCombineNullValue() {
        Properties properties1 = graphFactory().createProperties();
        properties1.put("name", BytesId.of("marko"));
        properties1.put("city", BytesId.of("Beijing"));

        Properties properties2 = graphFactory().createProperties();
        properties2.put("name", BytesId.of("josh"));
        properties2.put("age", BytesId.of("18"));

        PropertiesCombiner combiner = new MergeOldPropertiesCombiner();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(null, properties2, properties2);
        }, e -> {
            Assert.assertEquals("The combine parameter v1 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(properties1, null, properties2);
        }, e -> {
            Assert.assertEquals("The combine parameter v2 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(properties1, properties2, null);
        }, e -> {
            Assert.assertEquals("The combine parameter result can't be null",
                                e.getMessage());
        });
    }
}
