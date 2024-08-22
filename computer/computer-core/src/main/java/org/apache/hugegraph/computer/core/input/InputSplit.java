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

package org.apache.hugegraph.computer.core.input;

import java.util.Objects;

public class InputSplit {

    public static final InputSplit END_SPLIT = new InputSplit(null, null);

    // inclusive
    private final String start;
    // exclusive
    private final String end;

    public InputSplit(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String start() {
        return this.start;
    }

    public String end() {
        return this.end;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || this.getClass() != object.getClass()) {
            return false;
        }
        InputSplit other = (InputSplit) object;
        return Objects.equals(this.start, other.start) &&
               Objects.equals(this.end, other.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.start, this.end);
    }

    @Override
    public String toString() {
        return String.format("InputSplit{start='%s', end='%s'}",
                             this.start, this.end);
    }
}
