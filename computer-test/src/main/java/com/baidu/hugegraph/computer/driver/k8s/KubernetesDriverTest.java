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

import static io.fabric8.kubernetes.client.Config.KUBERNETES_KUBECONFIG_FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
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
import com.baidu.hugegraph.computer.k8s.operator.OperatorEntrypoint;
import com.baidu.hugegraph.computer.k8s.operator.config.OperatorOptions;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedClusterBuilder;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.api.model.NamedContextBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.internal.KubeConfigUtils;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;

public class KubernetesDriverTest {

    @Rule
    public KubernetesServer server = new KubernetesServer(true, true);

    private HashMap<String, String> options;
    private HugeConfig config;
    private KubernetesDriver driver;
    private OperatorEntrypoint entrypoint;
    private String namespace;

    public void updateOptions(String key, Object value) {
        this.config.clearProperty(key);
        this.config.addProperty(key, String.valueOf(value));
    }

    @Before
    public void setup() throws IOException {
        this.namespace = "test";

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
                         this.namespace);

        Config configuration = this.server.getClient().getConfiguration();
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        try {
            String absolutePath = tempFile.getAbsolutePath();

            this.options.put(KubeDriverOptions.KUBE_CONFIG.name(),
                             absolutePath);

            MapConfiguration mapConfig = new MapConfiguration(this.options);
            this.config = new HugeConfig(mapConfig);

            NamedCluster cluster = new NamedClusterBuilder()
                    .withName("kubernetes")
                    .withNewCluster()
                    .withServer(configuration.getMasterUrl())
                    .withInsecureSkipTlsVerify(configuration.isTrustCerts())
                    .withCertificateAuthorityData(configuration.getCaCertData())
                    .endCluster()
                    .build();
            NamedContext context = new NamedContextBuilder()
                    .withName("test@kubernetes")
                    .withNewContext()
                    .withCluster(cluster.getName())
                    .endContext()
                    .build();
            io.fabric8.kubernetes.api.model.Config config = Config.builder()
                    .withClusters(cluster)
                    .addToContexts(context)
                    .withCurrentContext(context.getName())
                    .build();
            KubeConfigUtils.persistKubeConfigIntoFile(config, absolutePath);
            System.setProperty(KUBERNETES_KUBECONFIG_FILE, absolutePath);

            this.driver = new KubernetesDriver(this.config);

            new Thread(()-> {
                System.setProperty(OperatorOptions.PROBE_PORT.name(), "9892");
                System.setProperty(OperatorOptions.WATCH_NAMESPACE.name(),
                                   this.namespace);
                this.entrypoint = new OperatorEntrypoint();
                this.entrypoint.start();
            }).start();
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
    }

    @After
    public void teardown() throws IOException {
        if (this.driver != null) {
            this.driver.close();
            this.driver = null;
        }

        this.entrypoint.shutdown();

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

    @Test
    public void testUploadAlgorithmJar() throws FileNotFoundException {
        this.updateOptions(KubeDriverOptions.BUILD_IMAGE_BASH_PATH.name(),
                           "conf/images/upload_test.sh");
        InputStream inputStream = new FileInputStream(
                                      "conf/images/test.jar");
        this.driver.uploadAlgorithmJar("PageRank", inputStream);

        File file = new File("conf/images/upload.txt");
        try {
            Assert.assertTrue(file.exists());
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testUploadAlgorithmJarWithError() throws FileNotFoundException {
        this.updateOptions(KubeDriverOptions.BUILD_IMAGE_BASH_PATH.name(),
                           "conf/images/upload_test-x.sh");
        InputStream inputStream = new FileInputStream(
                                      "conf/images/test.jar");

        Assert.assertThrows(RuntimeException.class, () -> {
            this.driver.uploadAlgorithmJar("PageRank", inputStream);
        }, e -> {
            Assert.assertContains("No such file", e.getMessage());
        });
    }

    @Test
    public void testSubmitJob() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank", params);
        NamespacedKubernetesClient client = this.server.getClient();
    }

    @Test
    public void testCan() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank", params);

        this.driver.cancelJob(jobId, params);
    }
}
