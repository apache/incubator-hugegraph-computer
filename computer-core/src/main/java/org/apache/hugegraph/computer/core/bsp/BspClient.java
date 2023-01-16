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

package org.apache.hugegraph.computer.core.bsp;

import java.util.List;

interface BspClient {

    /**
     * Return bsp server type, like etcd or zookeeper.
     */
    String type();

    /**
     * Get endpoint of the bsp server.
     */
    String endpoint();

    /**
     * Do initialization operation, like connect to etcd server.
     */
    void init(String namespace);

    /**
     * Close connection from bsp server.
     * Could not do any bsp operation after close is called.
     */
    void close();

    /**
     * Clean the bsp data of the job.
     */
    void clean();

    /**
     * Put key & value to the bsp server.
     */
    void put(String key, byte[] value);

    /**
     * Get value by key from the bsp server.
     */
    byte[] get(String key);

    /**
     * Get value by key from the bsp server with timout.
     */
    byte[] get(String key, long timeout, long logInterval);

    /**
     * Get expected count of child values of the key. If
     * there is no count of sub keys, wait at most timeout milliseconds.
     * @param key the key
     * @param count the expected count of values to be get
     * @param timeout the max wait time
     * @param logInterval the interval in ms to log message
     * @return the list of values which key with specified prefix
     */
    List<byte[]> getChildren(String key, int count,
                             long timeout, long logInterval);
}
