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

package org.apache.hugegraph.computer.k8s;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.driver.ComputerDriverException;
import org.apache.hugegraph.computer.driver.DefaultJobState;
import org.apache.hugegraph.computer.driver.JobObserver;
import org.apache.hugegraph.computer.driver.JobState;
import org.apache.hugegraph.computer.driver.JobStatus;
import org.apache.hugegraph.computer.k8s.config.KubeDriverOptions;
import org.apache.hugegraph.computer.k8s.config.KubeSpecOptions;
import org.apache.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import org.apache.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import org.apache.hugegraph.computer.k8s.driver.KubernetesDriver;
import org.apache.hugegraph.computer.k8s.util.KubeUtil;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

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
    @Override
    public void setup() throws IOException {
        this.initConfig();
        Config configuration = this.server.getClient().getConfiguration();
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        try {
            String absolutePath = tempFile.getAbsolutePath();
            this.updateOptions(KubeDriverOptions.KUBE_CONFIG.name(), absolutePath);
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
                                                                  .withCurrentContext(
                                                                          context.getName())
                                                                  .build();
            KubeConfigUtils.persistKubeConfigIntoFile(config, absolutePath);
            System.setProperty(Config.KUBERNETES_KUBECONFIG_FILE, absolutePath);

            @SuppressWarnings("resource")
            DefaultKubernetesClient client = new DefaultKubernetesClient();
            this.kubeClient = client.inNamespace(this.namespace);

            this.initPullSecret();
            this.initKubernetesDriver();
            this.initOperator();
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
    }

    @After
    @Override
    public void teardown() throws InterruptedException, ExecutionException {
        super.teardown();
    }

    @Test
    public void testConstruct() {
        String namespace = Whitebox.getInternalState(this.driver, "namespace");
        HugeConfig conf = Whitebox.getInternalState(this.driver, "conf");
        Object operation = Whitebox.getInternalState(this.driver, "operation");
        MutableBoolean watchActive = Whitebox.getInternalState(this.driver, "watchActive");
        Assert.assertTrue(watchActive.booleanValue());
        Assert.assertEquals(namespace, "test");
        Assert.assertNotNull(conf);
        Assert.assertNotNull(operation);

        final int workerInstances = 2;
        this.updateOptions(KubeSpecOptions.WORKER_INSTANCES.name(), workerInstances);
        Map<String, Object> defaultSpec = Whitebox.invoke(KubernetesDriver.class,
                                                          "defaultSpec", this.driver);
        String workerInstancesKey = KubeUtil.covertSpecKey(KubeSpecOptions.WORKER_INSTANCES.name());
        Assert.assertEquals(defaultSpec.get(workerInstancesKey), workerInstances);
    }

    @Test
    public void testUploadAlgorithmJar() throws FileNotFoundException {
        Whitebox.setInternalState(this.driver, "bashPath", "conf/images/docker_push_test.sh");
        Whitebox.setInternalState(this.driver, "registry", "registry.hub.docker.com");
        String url = "https://github.com/apache/hugegraph-doc/raw/" +
                     "binary-1.0/dist/computer/test.jar";
        String path = "conf/images/test.jar";
        downloadFileByUrl(url, path);

        InputStream inputStream = new FileInputStream(path);
        this.driver.uploadAlgorithmJar("PageRank", inputStream);

        File file = new File("/tmp/upload.txt");
        try {
            Assert.assertTrue(file.exists());
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testUploadAlgorithmJarWithError() {
        Whitebox.setInternalState(this.driver, "bashPath", "conf/images/upload_test-x.sh");
        String url = "https://github.com/apache/hugegraph-doc/raw/" +
                     "binary-1.0/dist/computer/test.jar";
        String path = "conf/images/test.jar";
        downloadFileByUrl(url, path);

        try (InputStream inputStream = new FileInputStream(path)) {
            Assert.assertThrows(ComputerDriverException.class, () -> {
                this.driver.uploadAlgorithmJar("PageRank", inputStream);
            }, e -> {
                ComputerDriverException exception = (ComputerDriverException) e;
                Assert.assertContains("No such file", exception.rootCause().getMessage());
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSubmitJob() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank", params);
        HugeGraphComputerJob computerJob = this.operation.withName(KubeUtil.crName(jobId)).get();
        Assert.assertNotNull(computerJob);
        Assert.assertEquals(computerJob.getSpec().getAlgorithmName(), "PageRank");
        Assert.assertEquals(computerJob.getSpec().getJobId(), jobId);
    }

    @Test
    public void testCancelJob() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank2", params);

        String crName = KubeUtil.crName(jobId);
        HugeGraphComputerJob computerJob = this.operation.withName(crName).get();
        Assert.assertNotNull(computerJob);

        UnitTestBase.sleep(1000L);

        this.driver.cancelJob(jobId, params);
        HugeGraphComputerJob canceledComputerJob = this.operation.withName(crName).get();
        Assert.assertNull(canceledComputerJob);
        Assert.assertNull(this.driver.jobState(jobId, params));
    }

    @Test
    public void testWatchJobAndCancel() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");
        String jobId = this.driver.submitJob("PageRank3", params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params, jobObserver);

        Mockito.verify(jobObserver, Mockito.timeout(5000L).atLeast(1))
               .onJobStateChanged(Mockito.any(DefaultJobState.class));

        future.getNow(null);

        MutableBoolean watchActive = Whitebox.getInternalState(this.driver, "watchActive");
        watchActive.setFalse();
        this.driver.waitJobAsync(jobId, params, jobObserver);

        this.driver.cancelJob(jobId, params);
        UnitTestBase.sleep(1000L);

        CompletableFuture<Void> future2 = this.driver.waitJobAsync(jobId, params, jobObserver);
        Assert.assertNull(future2);
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
        waits.put("test-123", Pair.of(new CompletableFuture<>(), Mockito.mock(JobObserver.class)));

        AbstractWatchManager<HugeGraphComputerJob> watch = Whitebox.getInternalState(this.driver,
                                                                                     "watch");
        Watcher<HugeGraphComputerJob> watcher = Whitebox.getInternalState(watch, "watcher");

        watcher.eventReceived(Watcher.Action.ADDED, null);
        watcher.eventReceived(Watcher.Action.ERROR, new HugeGraphComputerJob());
        HugeGraphComputerJob computerJob = new HugeGraphComputerJob();
        computerJob.setSpec(new ComputerJobSpec());
        watcher.eventReceived(Watcher.Action.MODIFIED, computerJob);

        WatcherException testClose = new WatcherException("test close");
        watcher.onClose(testClose);

        MutableBoolean watchActive = Whitebox.getInternalState(this.driver, "watchActive");
        Assert.assertFalse(watchActive.booleanValue());
    }

    @Test
    public void testCheckComputerConf() {
        Map<String, String> params = new HashMap<>();
        params.put(ComputerOptions.JOB_PARTITIONS_COUNT.name(), "9");
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "10");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.driver.submitJob("PageRank3", params);
        }, e -> {
            Assert.assertContains("The partitions count must be >= workers instances",
                                  e.getMessage()
            );
        });

        Map<String, String> defaultConf = Whitebox.getInternalState(this.driver, "defaultConf");
        defaultConf = new HashMap<>(defaultConf);
        defaultConf.remove(ComputerOptions.ALGORITHM_PARAMS_CLASS.name());
        Whitebox.setInternalState(this.driver, "defaultConf", defaultConf);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.driver.submitJob("PageRank3", params);
        }, e -> {
            Assert.assertContains("The [algorithm.params_class] options can't be null",
                                  e.getMessage()
            );
        });
    }

    public static void downloadFileByUrl(String url, String destPath) {
        int connectTimeout = 5000;
        int readTimeout = 10000;
        try {
            FileUtils.copyURLToFile(new URL(url), new File(destPath), connectTimeout, readTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
