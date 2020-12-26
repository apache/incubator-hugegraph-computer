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

package com.baidu.hugegraph.computer.core.common;

public class LongId extends LongValue implements Id<LongId> {

    public LongId() {
        super();
    }

    public LongId(long value) {
        super(value);
    }

    @Override
    public int compareTo(LongId o) {
        long result = this.value() - o.value();
        if (result > 0L) {
            return 1;
        } else if (result < 0L) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LongId)) {
            return false;
        }
        return ((LongId) obj).value() == this.value();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.value());
    }
}
