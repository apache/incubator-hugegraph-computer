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

package com.baidu.hugegraph.computer.core.graph.value;

import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.util.E;

public interface Value<T> extends Writable, Readable, Comparable<T> {

    /**
     * @return the value type of this instance
     */
    ValueType valueType();

    /**
     * Assign a new value to this object
     */
    void assign(Value<T> value);

    /**
     * @return copy a value instance of this object
     */
    Value<T> copy();

    /**
     * Check whether a value can be assigned to this object
     */
    default void checkAssign(Value<T> other) {
        if (other == null) {
            E.checkArgument(false, "Can't assign null to %s",
                            this.getClass().getSimpleName());
        } else if (!this.getClass().isAssignableFrom(other.getClass())) {
            E.checkArgument(false, "Can't assign '%s'(%s) to %s",
                            other, other.getClass().getSimpleName(),
                            this.getClass().getSimpleName());
        }
    }

    /**
     * @return value of this object
     */
    Object object();
}
