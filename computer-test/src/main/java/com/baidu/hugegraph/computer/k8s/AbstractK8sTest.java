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

package com.baidu.hugegraph.computer.k8s;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.configuration.MapConfiguration;
import org.junit.After;
import org.junit.Before;

import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.driver.config.ComputerOptions;
import com.baidu.hugegraph.computer.k8s.config.KubeDriverOptions;
import com.baidu.hugegraph.computer.k8s.config.KubeSpecOptions;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.driver.KubernetesDriver;
import com.baidu.hugegraph.computer.k8s.operator.OperatorEntrypoint;
import com.baidu.hugegraph.computer.k8s.operator.config.OperatorOptions;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.google.common.collect.Lists;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.Utils;

public abstract class AbstractK8sTest {

    protected String namespace = "test";
    protected HugeConfig config;
    protected KubernetesDriver driver;
    protected KubernetesClient kubeClient;
    protected OperatorEntrypoint entrypoint;
    protected Future<?> operatorFuture;
    protected MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
              Resource<HugeGraphComputerJob>> operation;

    protected static final String IMAGE_REPOSITORY_URL =
              "czcoder/hugegraph-computer-test";

    static {
        OptionSpace.register("computer-driver",
                             "com.baidu.hugegraph.computer.driver.config" +
                             ".ComputerOptions");
        OptionSpace.register("computer-k8s-driver",
                             "com.baidu.hugegraph.computer.k8s.config" +
                             ".KubeDriverOptions");
        OptionSpace.register("computer-k8s-spec",
                             "com.baidu.hugegraph.computer.k8s.config" +
                             ".KubeSpecOptions");
    }

    protected void updateOptions(String key, Object value) {
        this.config.clearProperty(key);
        this.config.addProperty(key, String.valueOf(value));
    }

    @Before
    public void setup() throws IOException {
        this.initConfig();
        this.kubeClient = new DefaultKubernetesClient()
                              .inNamespace(this.namespace);
        this.createCRD(this.kubeClient);
        this.initKubernetesDriver();
        this.initOperator();
    }

    @After
    public void teardown() throws IOException, ExecutionException,
                                  InterruptedException {
        this.driver.close();
        this.entrypoint.shutdown();
        this.operatorFuture.get();
        Set<String> keySet = OperatorOptions.instance().options().keySet();
        for (String key : keySet) {
            System.clearProperty(key);
        }
        System.clearProperty(Config.KUBERNETES_KUBECONFIG_FILE);
    }

    protected void initConfig() {
        HashMap<String, String> options = new HashMap<>();
        options.put(ComputerOptions.JOB_ID.name(),
                    KubeUtil.genJobId("PageRank"));
        options.put(ComputerOptions.JOB_WORKERS_COUNT.name(), "1");
        options.put(ComputerOptions.ALGORITHM_RESULT_CLASS.name(),
                    LongValue.class.getName());
        options.put(ComputerOptions.ALGORITHM_PARAMS_CLASS.name(),
                    "com.baidu.hugegraph.computer.core.config.Null");
        options.put(ComputerOptions.JOB_PARTITIONS_COUNT.name(),
                    "1000");
        options.put(ComputerOptions.BSP_ETCD_ENDPOINTS.name(),
                    "http://abc:8098");
        options.put(ComputerOptions.HUGEGRAPH_URL.name(),
                    "http://127.0.0.1:8080");
        options.put(KubeDriverOptions.NAMESPACE.name(),
                    this.namespace);
        options.put(KubeDriverOptions.LOG4J_XML_PATH.name(),
                    "conf/log4j2-test.xml");
        options.put(KubeDriverOptions.ENABLE_INTERNAL_ALGORITHM.name(),
                    "false");
        options.put(KubeDriverOptions.IMAGE_REPOSITORY_URL.name(),
                    IMAGE_REPOSITORY_URL);
        options.put(KubeDriverOptions.IMAGE_REPOSITORY_USERNAME.name(),
                    "hugegraph");
        options.put(KubeDriverOptions.IMAGE_REPOSITORY_PASSWORD.name(),
                    "hugegraph");
        options.put(KubeDriverOptions.INTERNAL_ALGORITHM_IMAGE_URL.name(),
                    IMAGE_REPOSITORY_URL + ":PageRank-latest");
        options.put(KubeSpecOptions.PULL_POLICY.name(), "IfNotPresent");
        options.put(KubeSpecOptions.JVM_OPTIONS.name(), "-Dlog4j2.debug=true");
        options.put(KubeSpecOptions.MASTER_COMMAND.name(), "[/bin/sh, -c]");
        options.put(KubeSpecOptions.WORKER_COMMAND.name(), "[/bin/sh, -c]");
        options.put(KubeSpecOptions.MASTER_ARGS.name(), "[echo master]");
        options.put(KubeSpecOptions.WORKER_ARGS.name(), "[echo worker]");
        MapConfiguration mapConfig = new MapConfiguration(options);
        this.config = new HugeConfig(mapConfig);
    }

    protected void initPullSecret() {
        String dockerServer = this.config.get(
                              KubeDriverOptions.IMAGE_REPOSITORY_URL);
        String username = this.config.get(
                          KubeDriverOptions.IMAGE_REPOSITORY_USERNAME);
        String password = this.config.get(
                          KubeDriverOptions.IMAGE_REPOSITORY_PASSWORD);
        Secret secret = KubeUtil.dockerRegistrySecret(this.namespace,
                                                      dockerServer,
                                                      username,
                                                      password);
        this.kubeClient.secrets().createOrReplace(secret);
        this.updateOptions(KubeDriverOptions.PULL_SECRET_NAMES.name(),
                           Lists.newArrayList(secret.getMetadata().getName()));
    }

    protected void initKubernetesDriver() {
        this.driver = new KubernetesDriver(this.config);
        this.operation = Whitebox.getInternalState(this.driver,
                                                   "operation");
    }

    protected void initOperator() {
        ExecutorService pool = ExecutorUtil.newFixedThreadPool("operator-test");
        this.operatorFuture = pool.submit(() -> {
            String watchNameSpace = Utils.getSystemPropertyOrEnvVar(
                                    "WATCH_NAMESPACE");
            if (!Objects.equals(watchNameSpace, Constants.ALL_NAMESPACE)) {
                System.setProperty("WATCH_NAMESPACE", this.namespace);
            } else {
                NamespaceBuilder namespaceBuilder = new NamespaceBuilder()
                        .withNewMetadata()
                        .withName(this.namespace)
                        .endMetadata();
                KubeUtil.ignoreExists(() -> {
                    return this.kubeClient.namespaces()
                                          .create(namespaceBuilder.build());
                });
            }
            this.entrypoint = new OperatorEntrypoint();
            this.entrypoint.start();
        });
        UnitTestBase.sleep(2000L);
    }

    private void createCRD(KubernetesClient client) {
        client.apiextensions().v1beta1()
              .customResourceDefinitions()
              .load(new File("../computer-k8s-operator/manifest" +
                             "/hugegraph-computer-crd.v1beta1.yaml"))
              .createOrReplace();
    }
}
