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
package org.apache.hugegraph.computer.core.util;

import java.io.Closeable;

import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class ShutdownHook {

    private static final Logger LOG = Log.logger(ShutdownHook.class);

    private volatile Thread threadShutdownHook;

    public boolean hook(Closeable hook) {
        if (hook == null) {
            return false;
        }

        this.threadShutdownHook = new Thread(() -> {
            try {
                hook.close();
            } catch (Throwable e) {
                LOG.warn("Failed to execute shutdown hook: {}",
                          e.getMessage(), e);
            }
        });
        Runtime.getRuntime().addShutdownHook(this.threadShutdownHook);
        return true;
    }

    public boolean unhook() {
        if (this.threadShutdownHook == null) {
            return false;
        }

        try {
            return Runtime.getRuntime()
                          .removeShutdownHook(this.threadShutdownHook);
        } finally {
            this.threadShutdownHook = null;
        }
    }
}
