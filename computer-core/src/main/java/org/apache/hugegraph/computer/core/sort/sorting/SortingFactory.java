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

import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;

public class SortingFactory {

    private static final SortingMode MODE = SortingMode.LOSER_TREE;

    public static <T> InputsSorting<T> createSorting(
                                       List<? extends Iterator<T>> inputs,
                                       SortingMode mode) {
        switch (mode) {
            case HEAP:
                return createHeapSorting(inputs);
            case LOSER_TREE:
                return createLoserTreeSorting(inputs);
            default:
                throw new ComputerException("Can't create sorting for '%s'",
                                            mode);
        }
    }

    public static <T> InputsSorting<T> createSorting(
                                       List<? extends Iterator<T>> inputs) {
        return createSorting(inputs, MODE);
    }

    private static <T> InputsSorting<T> createLoserTreeSorting(
                                        List<? extends Iterator<T>> inputs) {
        return new LoserTreeInputsSorting<>(inputs);
    }

    private static <T> InputsSorting<T> createHeapSorting(
                                        List<? extends Iterator<T>> inputs) {
        return new HeapInputsSorting<>(inputs);
    }
}
