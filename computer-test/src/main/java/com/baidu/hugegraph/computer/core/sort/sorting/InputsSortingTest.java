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

package com.baidu.hugegraph.computer.core.sort.sorting;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

public class InputsSortingTest {

    @Test
    public void testHeapInputsSorting() {
        InputsSorting<Integer> sorting;

        sorting = new HeapInputsSorting<>(TestData.data(), Integer::compareTo);
        this.assertSorted(TestData.data(), sorting);

        sorting = new HeapInputsSorting<>(TestData.dataWithEmpty());
        this.assertSorted(TestData.dataWithEmpty(), sorting);

        sorting = new HeapInputsSorting<>(TestData.dataEmpty());
        this.assertSorted(TestData.dataEmpty(), sorting);
    }

    @Test
    public void testLoserTreeInputsSorting() {
        InputsSorting<Integer> sorting;

        sorting = new LoserTreeInputsSorting<>(TestData.data(),
                                               Integer::compareTo);
        this.assertSorted(TestData.data(), sorting);

        sorting = new LoserTreeInputsSorting<>(TestData.dataWithEmpty());
        this.assertSorted(TestData.dataWithEmpty(), sorting);

        sorting = new LoserTreeInputsSorting<>(TestData.dataEmpty());
        this.assertSorted(TestData.dataEmpty(), sorting);
    }

    private void assertSorted(List<Iterator<Integer>> list,
                              InputsSorting<Integer> inputsSorting) {
        List<Integer> result = TestData.getSortedResult(list);
        Iterator<Integer> sortedResult = result.iterator();

        while (inputsSorting.hasNext() && sortedResult.hasNext()) {
            Assert.assertEquals(inputsSorting.next(), sortedResult.next());
        }

        Assert.assertFalse(inputsSorting.hasNext());
        Assert.assertFalse(sortedResult.hasNext());
        Assert.assertThrows(NoSuchElementException.class, inputsSorting::next);
    }
}
