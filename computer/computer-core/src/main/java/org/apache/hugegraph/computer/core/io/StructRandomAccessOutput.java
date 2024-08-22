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

import java.io.IOException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.testutil.Whitebox;

@SuppressWarnings("deprecation") // StringEscapeUtils
public class StructRandomAccessOutput implements RandomAccessOutput {

    private final RandomAccessOutput output;

    public StructRandomAccessOutput(RandomAccessOutput output) {
        this.output = output;
    }

    @Override
    public long position() {
        return this.output.position();
    }

    @Override
    public void seek(long position) throws IOException {
        this.output.seek(position);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.output.skip(n);
    }

    @Override
    public void write(RandomAccessInput input, long offset, long length)
                      throws IOException {
        if (UnsafeBytesInput.class == input.getClass()) {
            byte[] buffer = Whitebox.getInternalState(input, "buffer");
            this.write(buffer, (int) offset, (int) length);
        } else {
            input.seek(offset);
            byte[] bytes = input.readBytes((int) length);
            this.write(bytes);
        }
    }

    @Override
    public void write(int b) throws IOException {
        this.writeNumber(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.writeString(StringEncodeUtil.encodeBase64(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byte[] dest = new byte[len];
        System.arraycopy(b, off, dest, 0, len);
        this.writeString(StringEncodeUtil.encodeBase64(dest));
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.writeRawString(v ? "true" : "false");
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.writeNumber(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        this.writeString(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        this.writeString(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        this.writeString(s);
    }

    @Override
    public void writeFixedInt(int v) throws IOException {
        this.writeInt(v);
    }

    @Override
    public void writeFixedInt(long position, int v) throws IOException {
        long oldPosition = this.position();
        this.seek(position);
        this.writeInt(v);
        this.seek(oldPosition);
    }

    @Override
    public void close() throws IOException {
        this.output.close();
    }

    protected void writeNumber(Number number) throws IOException {
        this.output.writeBytes(number.toString());
    }

    protected void writeRawString(String s) throws IOException {
        this.output.writeBytes(s);
    }

    protected void writeString(String s) throws IOException {
        this.output.writeBytes("\"");
        this.output.writeBytes(StringEscapeUtils.escapeJson(s));
        this.output.writeBytes("\"");
    }
}
