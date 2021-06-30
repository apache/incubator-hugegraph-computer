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

package com.baidu.hugegraph.computer.driver.k8s;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.driver.config.ComputerOptions;
import com.baidu.hugegraph.computer.k8s.config.KubeDriverOptions;
import com.baidu.hugegraph.computer.k8s.config.KubeSpecOptions;
import com.baidu.hugegraph.computer.k8s.driver.KubernetesDriver;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;

public class KubernetesDriverTest {

    @Rule
    public KubernetesServer server = new KubernetesServer(true, true);

    private HashMap<String, String> options;
    private HugeConfig config;
    private KubernetesDriver driver;

    public void updateOptions(String key, Object value) {
        this.config.clearProperty(key);
        this.config.addProperty(key, String.valueOf(value));
    }

    @Before
    public void setup() {
        this.options = new HashMap<>();
        this.options.put(ComputerOptions.JOB_ID.name(),
                         KubeUtil.genJobId("PageRank"));
        this.options.put(ComputerOptions.JOB_WORKERS_COUNT.name(), "1");
        this.options.put(ComputerOptions.ALGORITHM_RESULT_CLASS.name(),
                         LongValue.class.getName());
        this.options.put(ComputerOptions.BSP_ETCD_ENDPOINTS.name(),
                         "http://abc:8098");
        this.options.put(ComputerOptions.HUGEGRAPH_URL.name(),
                         "http://127.0.0.1:8080");
        this.options.put(KubeDriverOptions.NAMESPACE.name(),
                         "test");

        MapConfiguration mapConfig = new MapConfiguration(this.options);
        this.config = new HugeConfig(mapConfig);
        NamespacedKubernetesClient client = this.server.getClient();
        this.driver = new KubernetesDriver(this.config, client);
    }

    @After
    public void teardown() throws IOException {
        if (this.driver != null) {
            this.driver.close();
            this.driver = null;
        }

        this.server.getMockServer().close();
    }

    @Test
    public void testConstruct() {
        String namespace = Whitebox.getInternalState(this.driver, "namespace");
        HugeConfig conf = Whitebox.getInternalState(this.driver, "conf");
        Object operation = Whitebox.getInternalState(this.driver, "operation");
        MutableBoolean watchActive = Whitebox.getInternalState(
                                     this.driver, "watchActive");
        Assert.assertTrue(watchActive.booleanValue());
        Assert.assertEquals(namespace, "test");
        Assert.assertNotNull(conf);
        Assert.assertNotNull(operation);

        final int workerInstances = 2;
        this.updateOptions(KubeSpecOptions.WORKER_INSTANCES.name(),
                           workerInstances);
        this.updateOptions(KubeSpecOptions.WORKER_INSTANCES.name(),
                           workerInstances);
        Map<String, Object> defaultSpec = Whitebox.invoke(
                                          KubernetesDriver.class,
                                          "defaultSpec",
                                          this.driver);
        String workerInstancesKey = KubeUtil.covertSpecKey(
                                    KubeSpecOptions.WORKER_INSTANCES.name());
        Assert.assertEquals(defaultSpec.get(workerInstancesKey),
                            workerInstances);
    }
}
