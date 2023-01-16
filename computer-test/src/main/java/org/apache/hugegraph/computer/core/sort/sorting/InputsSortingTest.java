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

package org.apache.hugegraph.computer.core.sort.sorting;

import java.util.List;

import org.junit.Test;

public class InputsSortingTest {
    
    private final SortingMode mode;
    
    protected InputsSortingTest(SortingMode mode) {
        this.mode = mode;
    }
    
    @Test
    public void testSorting() {
        InputsSorting<Integer> sorting;
        sorting = SortingFactory.createSorting(TestData.data(), this.mode);
        TestData.assertSorted(TestData.data(), sorting);

        sorting = SortingFactory.createSorting(TestData.dataWithEmpty(),
                                               this.mode);
        TestData.assertSorted(TestData.dataWithEmpty(), sorting);

        sorting = SortingFactory.createSorting(TestData.dataEmpty(),
                                               this.mode);
        TestData.assertSorted(TestData.dataEmpty(), sorting);

        sorting = SortingFactory.createSorting(TestData.sameDataLists(),
                                               this.mode);
        TestData.assertSorted(TestData.sameDataLists(), sorting);
    }

    @Test
    public void testWithRandomLists() {
        List<List<Integer>> randomLists = TestData.randomSortedLists(1000, 50);
        InputsSorting<Integer> sorting;
        sorting = SortingFactory.createSorting(
                  TestData.toIterators(randomLists), this.mode);
        TestData.assertSorted(TestData.toIterators(randomLists), sorting);
    }

    public static class LoserTreeInputsSortingTest extends InputsSortingTest {

        public LoserTreeInputsSortingTest() {
            super(SortingMode.LOSER_TREE);
        }
    }

    public static class HeapInputsSortingTest extends InputsSortingTest {

        public HeapInputsSortingTest() {
            super(SortingMode.HEAP);
        }
    }
}
