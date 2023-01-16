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

package org.apache.hugegraph.computer.core.store.file.seqfile;

import java.io.Closeable;
import java.io.IOException;

public interface BitsFileWriter extends Closeable {

    /**
     * Write 1 boolean to file, file is a directory.
     * Use 1 long store 64 booleans, write from low to high.
     * @param value boolean data
     * @throws IOException
     */
    void writeBoolean(boolean value) throws IOException;

    /**
     * Write 1 long to buffer when invoke writeBoolean 64 times.
     * This method will write the buffer to the file, but the part of written
     * not enough 64 times will not be written to the file.
     * @throws IOException
     */
    void flush() throws IOException;

    void close() throws IOException;
}
