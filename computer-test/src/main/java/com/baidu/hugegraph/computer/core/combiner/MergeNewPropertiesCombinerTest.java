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

package com.baidu.hugegraph.computer.core.combiner;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.testutil.Assert;

public class MergeNewPropertiesCombinerTest {

    @Test
    public void testCombine() {
        Properties properties1 = new DefaultProperties();
        properties1.put("name", new Utf8Id("marko").idValue());
        properties1.put("city", new Utf8Id("Beijing").idValue());

        Properties properties2 = new DefaultProperties();
        properties2.put("name", new Utf8Id("josh").idValue());
        properties2.put("age", new Utf8Id("18").idValue());

        Properties expect = new DefaultProperties();
        expect.put("name", new Utf8Id("marko").idValue());
        expect.put("age", new Utf8Id("18").idValue());
        expect.put("city", new Utf8Id("Beijing").idValue());

        PropertiesCombiner combiner = new MergeNewPropertiesCombiner();
        Properties properties = combiner.combine(properties1, properties2);
        Assert.assertEquals(expect, properties);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(null, properties2);
        }, e -> {
            Assert.assertEquals("The parameter v1 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(properties1, null);
        }, e -> {
            Assert.assertEquals("The parameter v2 can't be null",
                                e.getMessage());
        });
    }

    @Test
    public void testCombineNull() {
        Properties properties1 = new DefaultProperties();
        properties1.put("name", new Utf8Id("marko").idValue());
        properties1.put("city", new Utf8Id("Beijing").idValue());

        Properties properties2 = new DefaultProperties();
        properties2.put("name", new Utf8Id("josh").idValue());
        properties2.put("age", new Utf8Id("18").idValue());


        PropertiesCombiner combiner = new MergeNewPropertiesCombiner();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(null, properties2);
        }, e -> {
            Assert.assertEquals("The parameter v1 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(properties1, null);
        }, e -> {
            Assert.assertEquals("The parameter v2 can't be null",
                                e.getMessage());
        });
    }
}
