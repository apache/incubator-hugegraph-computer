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

package org.apache.hugegraph.computer.k8s.operator.common;

import java.time.Duration;
import java.util.Objects;

public class OperatorResult {

    private final boolean requeue;
    private final Duration requeueAfter;

    public static final OperatorResult NO_REQUEUE = new OperatorResult(false);
    public static final OperatorResult REQUEUE = new OperatorResult(true);

    public OperatorResult(boolean requeue) {
        this.requeue = requeue;
        this.requeueAfter = Duration.ZERO;
    }

    public boolean requeue() {
        return this.requeue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OperatorResult)) {
            return false;
        }

        final OperatorResult result = (OperatorResult) o;
        return this.requeue == result.requeue &&
               Objects.equals(this.requeueAfter, result.requeueAfter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.requeue, this.requeueAfter);
    }
}
