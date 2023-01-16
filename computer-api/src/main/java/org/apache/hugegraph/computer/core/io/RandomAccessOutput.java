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
import java.io.DataOutput;
import java.io.IOException;

public interface RandomAccessOutput extends DataOutput, Closeable {

    /**
     * Get the current write pointer position
     * @return the current postion
     */
    long position();

    /**
     * Move write pointer to specified position
     * @param position the new postion
     */
    void seek(long position) throws IOException;

    /**
     * Skip {@code n} bytes.
     * @return the position before skip.
     */
    long skip(long n) throws IOException;

    /**
     * Read some bytes from the input in range [offset, offset + length],
     * then write the bytes to this output at current position
     * @param input the source input
     * @param offset the start offset to read from input
     * @param length the total length to read from input
     */
    void write(RandomAccessInput input, long offset, long length)
               throws IOException;

    /**
     * At current position, write an int value that fixed in 4 bytes length
     * @param v the int value write
     */
    void writeFixedInt(int v) throws IOException;

    /**
     * Seek to specified position, and write an int value that fixed in 4
     * bytes length, then seek back to old postion
     * @param position the new postion to write
     * @param v the int value write
     */
    void writeFixedInt(long position, int v) throws IOException;
}
