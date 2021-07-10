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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.computer.driver.DefaultJobState;
import com.baidu.hugegraph.computer.driver.JobObserver;
import com.baidu.hugegraph.computer.driver.JobState;
import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.computer.k8s.config.KubeDriverOptions;
import com.baidu.hugegraph.computer.k8s.config.KubeSpecOptions;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.driver.KubernetesDriver;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedClusterBuilder;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.api.model.NamedContextBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.internal.AbstractWatchManager;
import io.fabric8.kubernetes.client.internal.KubeConfigUtils;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;

public class KubernetesDriverTest extends AbstractK8sTest {

    @Rule
    public KubernetesServer server = new KubernetesServer(true, true);

    @Before
    public void setup() throws IOException {
        this.initConfig();
        Config configuration = this.server.getClient().getConfiguration();
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        try {
            String absolutePath = tempFile.getAbsolutePath();
            this.updateOptions(KubeDriverOptions.KUBE_CONFIG.name(),
                               absolutePath);
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
            System.setProperty(Config.KUBERNETES_KUBECONFIG_FILE, absolutePath);

            this.kubeClient = new DefaultKubernetesClient()
                                  .inNamespace(this.namespace);

            this.initPullSecret();
            this.initKubernetesDriver();
            this.initOperator();
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
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
        this.updateOptions(KubeDriverOptions.IMAGE_REPOSITORY_REGISTRY.name(),
                           "registry.hub.docker.com");
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
        HugeGraphComputerJob computerJob = this.operation
                                               .withName(KubeUtil.crName(jobId))
                                               .get();
        Assert.assertNotNull(computerJob);
        Assert.assertEquals(computerJob.getSpec().getAlgorithmName(),
                            "PageRank");
        Assert.assertEquals(computerJob.getSpec().getJobId(), jobId);
    }

    @Test
    public void testCancelJob() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank2", params);

        String crName = KubeUtil.crName(jobId);
        HugeGraphComputerJob computerJob = this.operation.withName(crName)
                                                         .get();
        Assert.assertNotNull(computerJob);

        UnitTestBase.sleep(1000L);

        this.driver.cancelJob(jobId, params);
        HugeGraphComputerJob canceledComputerJob = this.operation
                                                       .withName(crName)
                                                       .get();
        Assert.assertNull(canceledComputerJob);
        Assert.assertNull(this.driver.jobState(jobId, params));
    }

    @Test
    public void testWaitJobAndCancel() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank3", params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            this.driver.waitJob(jobId, params, jobObserver);
        });

        Mockito.verify(jobObserver, Mockito.timeout(5000L).atLeast(1))
               .onJobStateChanged(Mockito.any(DefaultJobState.class));

        future.getNow(null);

        MutableBoolean watchActive = Whitebox.getInternalState(this.driver,
                                                               "watchActive");
        watchActive.setFalse();
        this.driver.waitJob(jobId, params, jobObserver);

        this.driver.cancelJob(jobId, params);
        this.driver.waitJob(jobId, params, jobObserver);
    }

    @Test
    public void testJobState() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank4", params);

        JobState jobState = this.driver.jobState(jobId, params);
        Assert.assertNotNull(jobState);
        Assert.assertEquals(JobStatus.INITIALIZING, jobState.jobStatus());
    }

    @Test
    public void testOnClose() {
        Map<String, Pair<CompletableFuture<Void>, JobObserver>> waits =
        Whitebox.getInternalState(this.driver, "waits");
        waits.put("test-123", Pair.of(new CompletableFuture<>(),
                                      Mockito.mock(JobObserver.class)));

        AbstractWatchManager<HugeGraphComputerJob> watch =
                                                   Whitebox.getInternalState(
                                                   this.driver, "watch");
        Watcher<HugeGraphComputerJob> watcher = Whitebox.getInternalState(
                                                watch, "watcher");

        watcher.eventReceived(Watcher.Action.ADDED, null);
        watcher.eventReceived(Watcher.Action.ERROR, new HugeGraphComputerJob());
        HugeGraphComputerJob computerJob = new HugeGraphComputerJob();
        computerJob.setSpec(new ComputerJobSpec());
        watcher.eventReceived(Watcher.Action.MODIFIED, computerJob);

        WatcherException testClose = new WatcherException("test close");
        watcher.onClose(testClose);

        MutableBoolean watchActive = Whitebox.getInternalState(this.driver,
                                                               "watchActive");
        Assert.assertFalse(watchActive.booleanValue());
    }
}
