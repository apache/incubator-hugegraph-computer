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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutputImpl;

public final class IOFactory {

    public static BytesOutput createBytesOutput(int size) {
        return new OptimizedBytesOutput(size);
    }

    public static BytesInput createBytesInput(byte[] buffer) {
        return new OptimizedBytesInput(buffer);
    }

    public static BytesInput createBytesInput(byte[] buffer, int limit) {
        return new OptimizedBytesInput(buffer, limit);
    }

    public static BytesInput createBytesInput(byte[] buffer, int position,
                                              int limit) {
        return new OptimizedBytesInput(buffer, position, limit);
    }

    public static RandomAccessOutput createFileOutput(File file)
                                     throws IOException {
        return new OptimizedBytesOutput(new BufferedFileOutput(file));
    }

    public static RandomAccessInput createFileInput(File file)
                                    throws IOException {
        return new OptimizedBytesInput(new BufferedFileInput(file));
    }

    public static RandomAccessOutput createStreamOutput(OutputStream stream)
                                     throws IOException {
        return new OptimizedBytesOutput(new BufferedStreamOutput(stream));
    }

    public static RandomAccessInput createStreamInput(InputStream stream)
                                    throws IOException {
        return new OptimizedBytesInput(new BufferedStreamInput(stream));
    }

    public static GraphOutput createGraphOutput(ComputerContext context,
                                                OutputFormat format,
                                                RandomAccessOutput out) {
        switch (format) {
            case BIN:
                EntryOutput entryOutput = new EntryOutputImpl(out);
                return new StreamGraphOutput(context, entryOutput);
            case CSV:
                StructRandomAccessOutput srao;
                srao = new StructRandomAccessOutput(out);
                return new CsvStructGraphOutput(context, srao);
            case JSON:
                srao = new StructRandomAccessOutput(out);
                return new JsonStructGraphOutput(context, srao);
            default:
                throw new ComputerException("Can't create GraphOutput for %s",
                                            format);
        }
    }
}
