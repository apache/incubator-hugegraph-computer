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

package org.apache.hugegraph.computer.dist;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.master.MasterService;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.util.ComputerContextUtil;
import org.apache.hugegraph.computer.core.worker.WorkerService;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;

public class HugeGraphComputer {

    private static final Logger LOG = Log.logger(HugeGraphComputer.class);

    private static final String ROLE_MASTER = "master";
    private static final String ROLE_WORKER = "worker";

    /**
     *  Some class must be load first, in order to invoke static method to init;
     */
    private static void loadClass() throws ClassNotFoundException {
        Class.forName(IdType.class.getCanonicalName());
        Class.forName(MessageType.class.getCanonicalName());
        Class.forName(ValueType.class.getCanonicalName());
    }

    public static void main(String[] args) throws IOException,
                                                  ClassNotFoundException {
        Runtime.getRuntime().addShutdownHook(new Thread(LogManager::shutdown));

        E.checkArgument(ArrayUtils.getLength(args) == 3,
                        "Argument count must be three, " +
                        "the first is conf path;" +
                        "the second is role type;" +
                        "the third is drive type.");
        String role = args[1];
        E.checkArgument(!StringUtils.isEmpty(role),
                        "The role can't be null or emtpy, " +
                        "it must be either '%s' or '%s'",
                        ROLE_MASTER, ROLE_WORKER);
        setUncaughtExceptionHandler();
        loadClass();
        registerOptions();
        ComputerContext context = parseContext(args[0], role);
        switch (role) {
            case ROLE_MASTER:
                executeMasterService(context);
                break;
            case ROLE_WORKER:
                executeWorkerService(context);
                break;
            default:
                throw new IllegalArgumentException(
                          String.format("Unexpected role '%s'", role));
        }
    }

    protected static void setUncaughtExceptionHandler() {
        Thread.UncaughtExceptionHandler handler =
               Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(
               new PrintExceptionHandler(handler)
        );
    }

    private static class PrintExceptionHandler implements
                                               Thread.UncaughtExceptionHandler {

        private final Thread.UncaughtExceptionHandler handler;

        PrintExceptionHandler(Thread.UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            HugeGraphComputer.LOG.error("Failed to run service on {}, {}",
                                        t, e.getMessage(), e);
            if (this.handler != null) {
                this.handler.uncaughtException(t, e);
            }
        }
    }

    private static void executeWorkerService(ComputerContext context) {
        try (WorkerService workerService = new WorkerService()) {
            workerService.init(context.config());
            workerService.execute();
        }
    }

    private static void executeMasterService(ComputerContext context) {
        try (MasterService masterService = new MasterService()) {
            masterService.init(context.config());
            masterService.execute();
        }
    }

    private static ComputerContext parseContext(String conf, String role)
                                                throws IOException {
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(
                                            new FileReader(conf));
        properties.load(bufferedReader);
        properties.remove(RpcOptions.RPC_REMOTE_URL.name());
        if (ROLE_WORKER.equals(role)) {
            properties.remove(RpcOptions.RPC_SERVER_HOST.name());
            properties.remove(RpcOptions.RPC_SERVER_PORT.name());
        }
        ComputerContextUtil.initContext(properties);
        return ComputerContext.instance();
    }

    private static void registerOptions() {
        OptionSpace.register("computer",
                             "org.apache.hugegraph.computer.core.config." +
                             "ComputerOptions");
        OptionSpace.register("computer-rpc",
                             "org.apache.hugegraph.config.RpcOptions");
    }
}
