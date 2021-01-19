/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.core.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang3.StringEscapeUtils;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.util.StringEncoding;

public abstract class StructGraphOutput implements GraphOutput {

    protected final DataOutputStream out;

    public StructGraphOutput(DataOutputStream out) {
        this.out = out;
    }

    public abstract void writeObjectStart() throws IOException;

    public abstract void writeObjectEnd() throws IOException;

    public abstract void writeKey(String key) throws IOException;

    public abstract void writeJoiner() throws IOException;

    public abstract void writeSplitter() throws IOException;

    @Override
    public void writeId(Id id) throws IOException {
        this.writeKey("id");
        this.writeJoiner();
        id.write(this);
        this.writeSplitter();
    }

    @Override
    public void writeValue(Value value) throws IOException {
        // String key = config.get(RankKey);
        // TODO: read key from config
        this.writeKey("rank");
        this.writeJoiner();
        value.write(this);
        // TODO: The splitter maybe doesn't need if no properties need write
        this.writeSplitter();
    }

    @Override
    public void write(int b) throws IOException {
        this.writeNumber(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.writeString(StringEncoding.encodeBase64(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byte[] dest = new byte[len];
        System.arraycopy(b, off, dest, 0, len);
        this.writeString(StringEncoding.encodeBase64(dest));
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

    protected void writeNumber(Number number) throws IOException {
        this.out.writeUTF(number.toString());
    }

    protected void writeRawString(String s) throws IOException {
        this.out.writeUTF(s);
    }

    protected void writeString(String s) throws IOException {
        this.out.writeUTF("\"" + StringEscapeUtils.escapeJson(s) + "\"");
    }
}
