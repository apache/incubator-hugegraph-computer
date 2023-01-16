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

package org.apache.hugegraph.computer.core.graph.value;

import org.apache.hugegraph.computer.core.io.Readable;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.util.E;

public interface Value extends Writable, Readable, Comparable<Value> {

    /**
     * @return the value type of this instance
     */
    ValueType valueType();

    /**
     * Assign a new value to this object
     */
    void assign(Value value);

    /**
     * @return copy a value instance of this object
     */
    Value copy();

    /**
     * @return value of this object
     */
    Object value();

    /**
     * Check whether a value can be assigned to this object
     */
    default void checkAssign(Value other) {
        if (other == null) {
            E.checkArgument(false, "Can't assign null to %s",
                            this.getClass().getSimpleName());
        } else if (!this.getClass().isAssignableFrom(other.getClass())) {
            E.checkArgument(false, "Can't assign '%s'(%s) to %s",
                            other, other.getClass().getSimpleName(),
                            this.getClass().getSimpleName());
        }
    }

    default boolean isNumber() {
        return false;
    }

    /**
     * @return result string value of this object
     */
    default String string() {
        return String.valueOf(this.value());
    }

    /**
     * Value class with template parameter for simple subclass extension
     */
    interface Tvalue<T> extends Value {

        @Override
        T value();
    }

    /**
     * Value class with template parameter for composite subclass extension,
     * A customized Value class of the algorithm may need to extend this class.
     */
    interface CustomizeValue<T> extends Tvalue<T> {

        @Override
        default ValueType valueType() {
            return ValueType.CUSTOMIZE_VALUE;
        }

        @Override
        default void assign(Value value) {
            throw new UnsupportedOperationException("assign");
        }

        @Override
        default Value copy() {
            throw new UnsupportedOperationException("copy");
        }

        @Override
        default int compareTo(Value other) {
            throw new UnsupportedOperationException("compareTo");
        }
    }
}
