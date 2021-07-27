/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.hugegraph.computer.dist;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.util.ComputerContextUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerService;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class HugeGraphComputer {

    private static final Logger LOG = Log.logger(HugeGraphComputer.class);

    private static final String ROLE_MASTER = "master";
    private static final String ROLE_WORKER = "worker";

    static {
        loadClass();
    }

    /**
     *  Some class must be load first, in order to invoke static method to init;
     */
    private static void loadClass() {
        try {
            Class.forName(IdType.class.getCanonicalName());
            Class.forName(MessageType.class.getCanonicalName());
            Class.forName(ValueType.class.getCanonicalName());
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to init class: {}", e.getMessage(), e);
            throw new ComputerException("Failed to init class: %s",
                                        e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws IOException {
        E.checkArgument(ArrayUtils.getLength(args) == 3,
                        "Argument count must be three, " +
                        "the first is conf path;" +
                        "the second is role type;" +
                        "the third is drive type.");
        String role = args[1];
        E.checkArgument(ROLE_MASTER.equals(role) ||
                        ROLE_WORKER.equals(role),
                        "The role must be either " +
                        "%s or %s", ROLE_MASTER, ROLE_WORKER);
        registerOptions();
        ComputerContext context = parseContext(args[0]);
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

    private static void executeWorkerService(ComputerContext context) {
        WorkerService workerService = null;
        try {
            workerService = new WorkerService();
            workerService.init(context.config());
            workerService.execute();
        } catch (Throwable e) {
            LOG.error("Failed to execute worker service: {}",
                      e.getMessage(), e);
            throw e;
        } finally {
            if (workerService != null) {
                workerService.close();
            }
        }
    }

    private static void executeMasterService(ComputerContext context) {
        MasterService masterService = null;
        try {
            masterService = new MasterService();
            masterService.init(context.config());
            masterService.execute();
        } catch (Throwable e) {
            LOG.error("Failed to execute master service: {}",
                      e.getMessage(), e);
            throw e;
        } finally {
            if (masterService != null) {
                masterService.close();
            }
        }
    }

    private static ComputerContext parseContext(String conf)
                                   throws IOException {
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(
                                            new FileReader(conf));
        properties.load(bufferedReader);
        ComputerContextUtil.initContext(properties);
        ComputerContext context = ComputerContext.instance();
        return context;
    }

    private static void registerOptions() {
        OptionSpace.register("computer",
                             "com.baidu.hugegraph.computer.core.config." +
                             "ComputerOptions");
        OptionSpace.register("computer-rpc",
                             "com.baidu.hugegraph.config.RpcOptions");
    }
}
