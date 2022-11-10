package com.baidu.hugegraph.computer.core.util;

import java.io.Closeable;

import org.slf4j.Logger;

import org.apache.hugegraph.util.Log;

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
