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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public class TestDataUtil {
    public static final List<List<Integer>> multiWayData =
            ImmutableList.of(
                    ImmutableList.of(10, 29, 35),
                    ImmutableList.of(50),
                    ImmutableList.of(4, 29),
                    ImmutableList.of(5, 5, 10),
                    ImmutableList.of(11, 23)
            );

    public static final List<List<Integer>> multiWayDataHasEmpty =
            ImmutableList.of(
                    ImmutableList.of(10, 29, 35),
                    ImmutableList.of(),
                    ImmutableList.of(4, 29),
                    ImmutableList.of(11, 23),
                    ImmutableList.of()
            );

    public static List<Integer> getSortedResult(List<List<Integer>> list) {
        return cloneList(list).stream()
                              .flatMap(Collection::stream)
                              .sorted()
                              .collect(Collectors.toList());
    }

    public static List<? extends Iterator<Integer>> convert(
            List<List<Integer>> list) {
        return cloneList(list).stream()
                              .map(List::iterator)
                              .collect(Collectors.toList());
    }

    private static List<List<Integer>> cloneList(List<List<Integer>> list) {
        List<List<Integer>> result = new ArrayList<>();
        list.forEach(item -> {
            List<Integer> resultItem = new ArrayList<>(item);
            result.add(resultItem);
        });
        return result;
    }
}
