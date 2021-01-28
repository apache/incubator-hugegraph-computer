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

package com.baidu.hugegraph.computer.core.common;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.util.E;

public class ContainerInfo implements Readable, Writable {

    /*
     * There is only 1 master, so the id of master is no use.
     * The id of workers start from 0. The id is used to identify a worker.
     */
    private int id;
    private String hostname;
    private int rpcPort;
    private int dataPort;

    // For reflexion.
    public ContainerInfo() {
    }

    public ContainerInfo(int id, String hostname, int rpcPort, int dataPort) {
        E.checkArgumentNotNull(hostname, "The hostname can't be null");
        this.id = id;
        this.hostname = hostname;
        this.rpcPort = rpcPort;
        this.dataPort = dataPort;
    }

    public int id() {
        return this.id;
    }

    public String hostname() {
        return this.hostname;
    }

    public int rpcPort() {
        return this.rpcPort;
    }

    public int dataPort() {
        return this.dataPort;
    }

    @Override
    public void read(GraphInput in) throws IOException {
        this.id = in.readInt();
        this.hostname = in.readUTF();
        this.rpcPort = in.readInt();
        this.dataPort = in.readInt();
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.hostname);
        out.writeInt(this.rpcPort);
        out.writeInt(this.dataPort);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ContainerInfo)) {
            return false;
        }
        ContainerInfo other = (ContainerInfo) obj;
        return this.id == other.id &&
               this.hostname.equals(other.hostname) &&
               this.rpcPort == other.rpcPort &&
               this.dataPort == other.dataPort;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(this.id);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ContainerInfo{id=").append(this.id)
          .append(", hostname=").append(this.hostname)
          .append(", rpcPort=").append(this.rpcPort)
          .append(", dataPort=").append(this.dataPort)
          .append("}");
        return sb.toString();
    }
}
