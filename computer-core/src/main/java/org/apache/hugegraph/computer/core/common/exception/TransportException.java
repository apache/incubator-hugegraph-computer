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

import java.io.IOException;

/**
 * The transport network exception
 */
public class TransportException extends IOException {

    private static final long serialVersionUID = -6538118382075299762L;

    public static final int DEFAULT_CODE = 0;
    public static final int REMOTE_ERROR_CODE = 1;

    private final int errorCode;

    public TransportException(String message) {
        this(DEFAULT_CODE, message);
    }

    public TransportException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public TransportException(String message, Object... args) {
        this(DEFAULT_CODE, message, args);
    }

    public TransportException(int errorCode, String message, Object... args) {
        super(String.format(message, args));
        this.errorCode = errorCode;
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = DEFAULT_CODE;
    }

    public TransportException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
        this.errorCode = DEFAULT_CODE;
    }

    public Throwable rootCause() {
        return ComputerException.rootCause(this);
    }

    public int errorCode() {
        return this.errorCode;
    }
}
