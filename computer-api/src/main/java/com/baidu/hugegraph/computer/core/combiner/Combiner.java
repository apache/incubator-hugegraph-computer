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

import java.util.Iterator;

public interface Combiner<T> {

    /**
     * @return The name of the combiner.
     * @return class name by default.
     */
    default String name() {
        return this.getClass().getName();
    }

    /**
     * Combine v1 and v2, return the combined value. The combined value may
     * take use v1 or v2. The value of v1 and v2 may be updated. Should not
     * use v1 and v2 after combine them.
     */
    T combine(T v1, T v2);


    public static <T> T combineAll(Combiner<T> combiner, Iterator<T> values) {
        if (!values.hasNext()) {
            return null;
        }
        T result = values.next();
        while (values.hasNext()) {
            result = combiner.combine(result, values.next());
        }
        return result;
    }
}