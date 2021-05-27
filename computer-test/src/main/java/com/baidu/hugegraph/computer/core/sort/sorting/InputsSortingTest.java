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

public class InputsSortingTest {
    
    @Test
    public void testSorting() {
        testSorting(SortingMode.LOSER_TREE);
        testSorting(SortingMode.HEAP);
    }
    
    @Test
    public void testWithRandomLists() {
        testWithRandomLists(SortingMode.LOSER_TREE);
        testWithRandomLists(SortingMode.HEAP);
    }
    
    private static void testSorting(SortingMode mode) {
        InputsSorting<Integer> sorting;
        sorting = SortingFactory.createSorting(TestData.data(), mode);
        TestData.assertSorted(TestData.data(), sorting);

        sorting = SortingFactory.createSorting(TestData.dataWithEmpty(), mode);
        TestData.assertSorted(TestData.dataWithEmpty(), sorting);

        sorting = SortingFactory.createSorting(TestData.dataEmpty(), mode);
        TestData.assertSorted(TestData.dataEmpty(), sorting);

        sorting = SortingFactory.createSorting(TestData.sameDataLists(), mode);
        TestData.assertSorted(TestData.sameDataLists(), sorting);
    }

    private static void testWithRandomLists(SortingMode mode) {
        List<List<Integer>> randomLists = TestData.randomSortedLists(1000, 50);
        InputsSorting<Integer> sorting;
        sorting = SortingFactory.createSorting(
                TestData.toIterators(randomLists), mode);
        TestData.assertSorted(TestData.toIterators(randomLists), sorting);
    }
}
