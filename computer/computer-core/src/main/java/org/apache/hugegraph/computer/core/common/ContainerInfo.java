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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.util.JsonUtil;
import org.apache.hugegraph.util.E;

public class ContainerInfo implements Readable, Writable {

    public static final int MASTER_ID = 0;

    /*
     * There is only 1 master, and the id of master is 0.
     * The id of workers start from 1. The id is used to identify a worker.
     */
    private int id;
    private String hostname;
    private int rpcPort;
    private int dataPort;

    // For reflexion.
    public ContainerInfo() {
        this(-1, "", -1, -1);
    }

    public ContainerInfo(int id, String hostname, int rpcPort) {
        this(id, hostname, rpcPort, -1);
    }

    public ContainerInfo(String hostname, int rpcPort, int dataPort) {
        this(-1, hostname, rpcPort, dataPort);
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

    public void id(int id) {
        this.id = id;
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

    public String uniqueName() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.hostname)
          .append(":")
          .append(this.dataPort);
        return sb.toString();
    }

    public void updateAddress(InetSocketAddress address) {
        this.hostname = TransportUtil.host(address);
        this.dataPort = address.getPort();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.id = in.readInt();
        this.hostname = in.readUTF();
        this.rpcPort = in.readInt();
        this.dataPort = in.readInt();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
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
        return JsonUtil.toJsonWithClass(this);
    }
}
