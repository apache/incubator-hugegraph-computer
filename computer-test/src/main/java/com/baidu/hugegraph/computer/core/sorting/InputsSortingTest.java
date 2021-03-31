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

package com.baidu.hugegraph.computer.core.sorting;

import static com.baidu.hugegraph.computer.core.sorting.TestDataUtil.multiWayData;
import static com.baidu.hugegraph.computer.core.sorting.TestDataUtil.multiWayDataHasEmpty;

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.sorting.sorting.HeapInputsSorting;
import com.baidu.hugegraph.computer.core.sorting.sorting.InputsSorting;
import com.baidu.hugegraph.computer.core.sorting.sorting.LoserTreeInputsSorting;


public class InputsSortingTest {

    @Test
    public void testHeapInputsSorting() {
        InputsSorting<Integer> heapInputsSorting;

        heapInputsSorting =
                new HeapInputsSorting<>(TestDataUtil.convert(multiWayData));
        this.assertSorted(multiWayData, heapInputsSorting);

        heapInputsSorting = new HeapInputsSorting<>(
                TestDataUtil.convert(multiWayDataHasEmpty));

        this.assertSorted(multiWayDataHasEmpty, heapInputsSorting);
    }

    @Test
    public void testLoserTreeInputsSorting() {
        InputsSorting<Integer> loserTreeInputsSorting;

        loserTreeInputsSorting = new LoserTreeInputsSorting<>(
                TestDataUtil.convert(multiWayData));
        this.assertSorted(multiWayData, loserTreeInputsSorting);

        loserTreeInputsSorting = new LoserTreeInputsSorting<>(
                TestDataUtil.convert(multiWayDataHasEmpty));

        this.assertSorted(multiWayDataHasEmpty, loserTreeInputsSorting);
    }

    private void assertSorted(List<List<Integer>> list,
                              InputsSorting<Integer> inputsSorting) {
        Iterator<Integer> sortedResult =
                TestDataUtil.getSortedResult(list).iterator();

        while (inputsSorting.hasNext() && sortedResult.hasNext()) {
            Assert.assertEquals(inputsSorting.next(), sortedResult.next());
        }

        Assert.assertFalse(inputsSorting.hasNext());
        Assert.assertFalse(sortedResult.hasNext());
    }
}
