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

package org.apache.hugegraph.computer.core.combiner;

import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.util.E;

public class OverwriteCombiner<T extends Value> implements Combiner<T> {

    @Override
    public void combine(T v1, T v2, T result) {
        E.checkArgumentNotNull(v1, "The combine parameter v1 can't be null");
        E.checkArgumentNotNull(v2, "The combine parameter v2 can't be null");
        E.checkArgumentNotNull(result,
                               "The combine parameter result can't be null");
        result.assign(v2);
    }
}
