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

package org.apache.hugegraph.computer.core.common;

import org.apache.hugegraph.util.Bytes;

public final class Constants {

    public static final byte[] EMPTY_BYTES = new byte[0];

    public static final int BOOLEAN_LEN = 1;
    public static final int BYTE_LEN = Byte.BYTES;
    public static final int SHORT_LEN = Short.BYTES;
    public static final int INT_LEN = Integer.BYTES;
    public static final int LONG_LEN = Long.BYTES;
    public static final int CHAR_LEN = Character.BYTES;
    public static final int FLOAT_LEN = Float.BYTES;
    public static final int DOUBLE_LEN = Double.BYTES;

    public static final int UINT8_MAX = 0xff;
    public static final int UINT16_MAX = 0xffff;
    public static final long UINT32_MAX = 0xffffffffL;

    /*
     * The small buffer size for buffered input & output,
     * mainly used in input & output of memory
     */
    public static final int SMALL_BUF_SIZE = 32;

    /*
     * The big buffer size for buffered input & output,
     * mainly used in input & output of file and stream
     */
    public static final int BIG_BUF_SIZE = (int) Bytes.KB * 8;

    /*
     * The capacity of message queue
     */
    public static final int QUEUE_CAPACITY = 128;

    /*
     * The timeout in second for asynchronous tasks
     */
    public static final int FUTURE_TIMEOUT = 300;

    /*
     * The timeout in millisecond for thread-pool shutdown
     */
    public static final long SHUTDOWN_TIMEOUT = 5000L;

    // The mode to read a file
    public static final String FILE_MODE_READ = "r";

    // The mode to write a file
    public static final String FILE_MODE_WRITE = "rw";

    public static final String EMPTY_STR = "";

    public static final int INPUT_SUPERSTEP = -1;
}
