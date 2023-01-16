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

package org.apache.hugegraph.computer.core.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;

public interface RandomAccessInput extends DataInput, Closeable {

    /**
     * @return The current position.
     */
    long position();

    /**
     * Set current position to specified position, measured from the beginning
     * of input.
     * @throws IOException If can't seek to specified position.
     */
    void seek(long position) throws IOException;

    /**
     * Skip {@code n} bytes.
     * @return the position before skip. This is different from {@link
     * DataInput#skipBytes} and {@link java.io.InputStream#skip}, which
     * return the number of bytes actually skipped.
     */
    long skip(long n) throws IOException;

    /**
     * @return The total bytes size unread.
     * @throws IOException
     */
    long available() throws IOException;

    /**
     * Creates a new input that shares content like buffer or file,
     * but use independent position
     */
    RandomAccessInput duplicate() throws IOException;

    /**
     * Compare two inputs in the specified range.
     */
    int compare(long offset, long length,
                RandomAccessInput other, long otherOffset, long otherLength)
                throws IOException;

    /**
     * @return Read the byte array of size length from the current position
     * @throws IOException
     */
    default byte[] readBytes(int size) throws IOException {
        byte[] bytes = new byte[size];
        this.readFully(bytes);
        return bytes;
    }

    default int readFixedInt() throws IOException {
        return this.readInt();
    }
}
