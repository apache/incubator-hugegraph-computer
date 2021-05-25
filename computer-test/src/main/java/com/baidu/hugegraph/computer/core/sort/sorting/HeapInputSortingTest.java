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

import java.util.List;
import org.junit.Test;

public class HeapInputSortingTest {

    @Test
    public void testHeapInputsSorting() {
        InputsSorting<Integer> sorting;

        sorting = new HeapInputsSorting<>(TestData.data(), Integer::compareTo);
        TestData.assertSorted(TestData.data(), sorting);

        sorting = new HeapInputsSorting<>(TestData.dataWithEmpty());
        TestData.assertSorted(TestData.dataWithEmpty(), sorting);

        sorting = new HeapInputsSorting<>(TestData.dataEmpty());
        TestData.assertSorted(TestData.dataEmpty(), sorting);

        sorting = new HeapInputsSorting<>(TestData.sameDataLists());
        TestData.assertSorted(TestData.sameDataLists(), sorting);
    }

    @Test
    public void testWithRandomLists() {
        List<List<Integer>> randomLists = TestData.randomSortedLists(1000, 50);
        InputsSorting<Integer> sorting = new HeapInputsSorting<>(
                                         TestData.toIterators(randomLists));
        TestData.assertSorted(TestData.toIterators(randomLists), sorting);
    }
}
