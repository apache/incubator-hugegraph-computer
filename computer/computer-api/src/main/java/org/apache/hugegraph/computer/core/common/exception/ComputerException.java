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

package org.apache.hugegraph.computer.core.common.exception;

/**
 * The base class for all exception types
 */
public class ComputerException extends RuntimeException {

    private static final long serialVersionUID = 3621207523020113277L;

    public ComputerException(String message) {
        super(message);
    }

    public ComputerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ComputerException(String message, Object... args) {
        super(String.format(message, args));
    }

    public ComputerException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }

    public Throwable rootCause() {
        return rootCause(this);
    }

    public static Throwable rootCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
