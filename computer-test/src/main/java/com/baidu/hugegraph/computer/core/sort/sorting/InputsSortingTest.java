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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.testutil.Assert;

public class InputsSortingTest {

    private static final Logger LOG = LoggerFactory.getLogger(
                                      InputsSortingTest.class);

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

    @Test
    public void testLoserTree() {
        List<Iterator<Integer>> sources = TestData.sameDataLists();
        InputsSorting<Integer> sorting = new LoserTreeInputsSorting<>(sources);
        for (int i = 0; i < sources.size(); i++) {
            Assert.assertTrue(sorting.hasNext());
            Assert.assertEquals(1, sorting.next());
        }
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

    @Test
    public void testLoserTreeOrder() {
        StopWatch watcher = new StopWatch();
        Random random = new Random();
        List<List<Integer>> lists = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            List<Integer> list = new ArrayList<>();
            for (int j = 0; j < 500; j++) {
                list.add(random.nextInt(100));
            }
            lists.add(list);
        }

        List<Iterator<Integer>> sources = lists.stream()
                                               .peek(Collections::sort)
                                               .map(List::iterator)
                                               .collect(Collectors.toList());

        watcher.start();
        InputsSorting<Integer> sorting = SortingFactory.createSorting(sources);
        int last = 0;
        while (sorting.hasNext()) {
            Integer next = sorting.next();
            Assert.assertTrue(last <= next);
            last = next;
        }
        watcher.stop();

        LOG.info("testLoserTreeOrder cost time:{}", watcher.getTime());
    }
}
