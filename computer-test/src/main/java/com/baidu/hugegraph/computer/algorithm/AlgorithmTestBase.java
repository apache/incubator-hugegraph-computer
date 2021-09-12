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

package com.baidu.hugegraph.computer.algorithm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.computer.core.util.ComputerContextUtil;
import com.baidu.hugegraph.computer.core.worker.MockWorkerService;
import com.baidu.hugegraph.computer.core.worker.WorkerService;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.schema.EdgeLabel;
import com.baidu.hugegraph.structure.schema.PropertyKey;
import com.baidu.hugegraph.structure.schema.VertexLabel;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class AlgorithmTestBase extends UnitTestBase {

    public static HugeClient HG_CLIENT;

    @BeforeClass
    public static void setup() {
        HG_CLIENT = HugeClient.builder(
                    ComputerOptions.HUGEGRAPH_URL.defaultValue(),
                    ComputerOptions.HUGEGRAPH_GRAPH_NAME.defaultValue())
                              .build();
    }

    @AfterClass
    public static void teardown() {
        HG_CLIENT.close();
    }

    protected static void clearAll(List<String> edgeLabelNames,
                                   List<String> vertexLabelNames,
                                   List<String> propertyKeyNames) {
        clearData(edgeLabelNames, vertexLabelNames);
        clearSchema(edgeLabelNames, vertexLabelNames, propertyKeyNames);
    }

    protected static void clearData(List<String> edgeLabelNames,
                                    List<String> vertexLabelNames) {
        for (String edgeLabelName : edgeLabelNames) {
            List<Edge> edges = HG_CLIENT.graph().listEdges(edgeLabelName);
            edges.forEach(edge -> HG_CLIENT.graph().removeEdge(edge.id()));
        }

        for (String vertexLabelName : vertexLabelNames) {
            List<Vertex> vertices = HG_CLIENT.graph()
                                             .listVertices(vertexLabelName);
            vertices.forEach(vertex -> HG_CLIENT.graph()
                                                .removeVertex(vertex.id()));
        }
    }

    protected static void clearSchema(List<String> edgeLabelNames,
                                      List<String> vertexLabelNames,
                                      List<String> propertyKeyNames) {
        SchemaManager schema = HG_CLIENT.schema();

        List<EdgeLabel> edgeLabels = schema.getEdgeLabels(edgeLabelNames);
        edgeLabels.forEach(label -> schema.removeEdgeLabel(label.name()));

        List<VertexLabel> vertexLabels = schema.getVertexLabels(
                                                vertexLabelNames);
        vertexLabels.forEach(label -> schema.removeVertexLabel(label.name()));

        List<PropertyKey> propertyKeys = schema.getPropertyKeys(
                                                propertyKeyNames);
        propertyKeys.forEach(label -> schema.removePropertyKey(label.name()));
    }

    public static void runAlgorithm(String algorithmParams, String ... options)
                       throws InterruptedException {
        final Logger log = Log.logger(AlgorithmTestBase.class);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Throwable[] exceptions = new Throwable[2];

        pool.submit(() -> {
            WorkerService workerService = null;
            try {
                Map<String, String> params = new HashMap<>();
                params.put(RpcOptions.RPC_REMOTE_URL.name(),
                           "127.0.0.1:8090");
                params.put(ComputerOptions.JOB_ID.name(),
                           "local_002");
                params.put(ComputerOptions.JOB_WORKERS_COUNT.name(),
                           "1");
                params.put(ComputerOptions.TRANSPORT_SERVER_PORT.name(),
                           "8086");
                params.put(ComputerOptions.BSP_REGISTER_TIMEOUT.name(),
                           "100000");
                params.put(ComputerOptions.BSP_LOG_INTERVAL.name(),
                           "30000");
                params.put(ComputerOptions.BSP_MAX_SUPER_STEP.name(),
                           "10");
                params.put(ComputerOptions.ALGORITHM_PARAMS_CLASS.name(),
                           algorithmParams);
                Config config = ComputerContextUtil.initContext(params);
                if (options != null) {
                    for (int i = 0; i < options.length; i += 2) {
                        params.put(options[i], options[i + 1]);
                    }
                }
                workerService = new MockWorkerService();

                Thread.sleep(2000L);
                workerService.init(config);
                workerService.execute();
            } catch (Throwable e) {
                log.error("Failed to start worker", e);
                exceptions[0] = e;
            } finally {
                if (workerService != null) {
                    workerService.close();
                }
                countDownLatch.countDown();
            }
        });

        pool.submit(() -> {
            MasterService masterService = null;
            try {
                Map<String, String> params = new HashMap<>();
                params.put(RpcOptions.RPC_SERVER_HOST.name(),
                           "localhost");
                params.put(RpcOptions.RPC_SERVER_PORT.name(),
                           "8090");
                params.put(ComputerOptions.JOB_ID.name(),
                           "local_002");
                params.put(ComputerOptions.JOB_WORKERS_COUNT.name(),
                           "1");
                params.put(ComputerOptions.BSP_REGISTER_TIMEOUT.name(),
                           "100000");
                params.put(ComputerOptions.BSP_LOG_INTERVAL.name(),
                           "30000");
                params.put(ComputerOptions.BSP_MAX_SUPER_STEP.name(),
                           "10");
                params.put(ComputerOptions.ALGORITHM_PARAMS_CLASS.name(),
                           algorithmParams);
                if (options != null) {
                    for (int i = 0; i < options.length; i += 2) {
                        params.put(options[i], options[i + 1]);
                    }
                }

                Config config = ComputerContextUtil.initContext(params);

                masterService = new MasterService();

                masterService.init(config);
                masterService.execute();
            } catch (Throwable e) {
                log.error("Failed to start master", e);
                exceptions[1] = e;
            } finally {
                /*
                 * It must close the service first. The pool will be shutdown
                 * if count down is executed first, and the server thread in
                 * master service will not be closed.
                 */
                if (masterService != null) {
                    masterService.close();
                }
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        pool.shutdownNow();

        Assert.assertFalse(Arrays.asList(exceptions).toString(),
                           existError(exceptions));
    }
}
