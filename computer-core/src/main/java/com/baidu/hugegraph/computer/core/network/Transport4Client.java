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

package com.baidu.hugegraph.computer.core.network;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

/**
 * This is used for worker to send buffer to other worker. The whole process
 * contains several iteration. In one iteration {@link #startIteration} is
 * called only once. {@link #send} is called zero or more times.
 * {@link #finishIteration} is called only once.
 */
public interface Transport4Client {

    /**
     * Init the connection from client to server. This method is called only
     * once.
     * @throws IOException if can't create connection.
     */
    void init(String hostname, int port) throws IOException;

    /**
     * This method is called before a iteration.
     */
    void startIteration();

    /**
     * Send the buffer to the server. Block the caller if busy.
     * This method is called zero or many times in iteration.
     * @throws IOException if failed, the job will fail.
     */
    void send(byte messageType, int partition, ByteBuf buffer)
              throws IOException;

    /**
     * This method is called after a iteration.
     */
    void finishIteration() throws IOException;
}
