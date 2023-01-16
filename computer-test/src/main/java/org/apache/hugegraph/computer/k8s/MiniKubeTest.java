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

import static io.fabric8.kubernetes.client.Config.getKubeconfigFilename;
import static org.apache.hugegraph.computer.k8s.config.KubeDriverOptions.INTERNAL_ALGORITHMS;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.driver.DefaultJobState;
import org.apache.hugegraph.computer.driver.JobObserver;
import org.apache.hugegraph.computer.driver.JobStatus;
import org.apache.hugegraph.computer.k8s.config.KubeDriverOptions;
import org.apache.hugegraph.computer.k8s.config.KubeSpecOptions;
import org.apache.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import org.apache.hugegraph.computer.k8s.driver.KubernetesDriver;
import org.apache.hugegraph.computer.k8s.operator.common.AbstractController;
import org.apache.hugegraph.computer.k8s.util.KubeUtil;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Log;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

public class MiniKubeTest extends AbstractK8sTest {

    private static final Logger LOG = Log.logger(MiniKubeTest.class);

    public static final String ALGORITHM_NAME = INTERNAL_ALGORITHMS.defaultValue().get(0);

    @Before
    public void setup() throws IOException {
        try {
            String kubeconfigFilename = getKubeconfigFilename();
            File file = new File(kubeconfigFilename);
            Assert.assertTrue(file.exists());

            this.namespace = "minikube";
            System.setProperty("WATCH_NAMESPACE", Constants.ALL_NAMESPACE);
            super.setup();
        } catch (Throwable throwable) {
            LOG.error("Failed to setup MiniKubeTest ", throwable);
            throw throwable;
        }
    }

    @Test
    public void testProbe() throws IOException {
        UnitTestBase.sleep(1000L);

        HttpClient client = HttpClientBuilder.create().build();
        URI health = URI.create("http://localhost:9892/health");
        HttpGet request = new HttpGet(health);
        HttpResponse response = client.execute(request);
        Assert.assertEquals(HttpStatus.SC_OK,
                            response.getStatusLine().getStatusCode());

        URI ready = URI.create("http://localhost:9892/ready");
        HttpGet requestReady = new HttpGet(ready);
        HttpResponse responseReady = client.execute(requestReady);
        Assert.assertEquals(HttpStatus.SC_OK,
                            responseReady.getStatusLine().getStatusCode());
    }

    @Test
    public void testJobSucceed() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        params.put(ComputerOptions.TRANSPORT_SERVER_PORT.name(), "0");
        params.put(ComputerOptions.RPC_SERVER_PORT_NAME, "0");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.SUCCEEDED);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));

        future.cancel(true);
    }

    @Test
    public void testJobInternalSucceed() {
        Whitebox.setInternalState(this.driver, "enableInternalAlgorithm",
                                  true);
        Whitebox.setInternalState(this.driver, "internalAlgorithms",
                                  Lists.newArrayList("algorithm123"));

        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        params.put(ComputerOptions.TRANSPORT_SERVER_PORT.name(), "0");
        params.put(ComputerOptions.RPC_SERVER_PORT_NAME, "0");
        String jobId = this.driver.submitJob("algorithm123", params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.SUCCEEDED);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));

        future.cancel(true);
    }

    @Test
    public void testJobFailed() {
        super.updateOptions(KubeSpecOptions.MASTER_ARGS.name(),
                            Lists.newArrayList("cat xxx"));
        super.updateOptions(KubeSpecOptions.WORKER_ARGS.name(),
                            Lists.newArrayList("cat xxx"));
        Object defaultSpec = Whitebox.invoke(KubernetesDriver.class,
                                             "defaultSpec",
                                             this.driver);
        Whitebox.setInternalState(this.driver, "defaultSpec", defaultSpec);

        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.FAILED);
        Mockito.verify(jobObserver, Mockito.timeout(150000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));

        UnitTestBase.sleep(500L);

        String diagnostics = this.driver.diagnostics(jobId, params);
        Assert.assertContains("No such file or directory", diagnostics);

        future.cancel(true);
    }

    @Test
    public void testUnSchedulable() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        params.put(KubeSpecOptions.MASTER_CPU.name(), "10");
        params.put(KubeSpecOptions.MASTER_MEMORY.name(), "10Gi");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.FAILED);
        Mockito.verify(jobObserver, Mockito.timeout(30000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        String diagnostics = this.driver.diagnostics(jobId, params);
        Assert.assertContains("Unschedulable", diagnostics);

        future.cancel(true);
    }

    @Test
    public void testJobCancelled() {
        super.updateOptions(KubeSpecOptions.MASTER_ARGS.name(),
                            Lists.newArrayList("pwd && sleep 60"));
        super.updateOptions(KubeSpecOptions.WORKER_ARGS.name(),
                            Lists.newArrayList("pwd && sleep 60"));
        Object defaultSpec = Whitebox.invoke(KubernetesDriver.class,
                                             "defaultSpec",
                                             this.driver);
        Whitebox.setInternalState(this.driver, "defaultSpec", defaultSpec);

        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        this.driver.cancelJob(jobId, params);

        UnitTestBase.sleep(1500L);

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.CANCELLED);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));

        future.cancel(true);
    }

    @Test
    public void testTwiceCreate() {
        super.updateOptions(KubeSpecOptions.MASTER_ARGS.name(),
                            Lists.newArrayList("pwd && sleep 60"));
        super.updateOptions(KubeSpecOptions.WORKER_ARGS.name(),
                            Lists.newArrayList("pwd && sleep 60"));
        Object defaultSpec = Whitebox.invoke(KubernetesDriver.class,
                                             "defaultSpec",
                                             this.driver);
        Whitebox.setInternalState(this.driver, "defaultSpec", defaultSpec);

        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.RUNNING);
        Mockito.verify(jobObserver, Mockito.timeout(20000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        HugeGraphComputerJob computerJob = this.operation
                                               .withName(KubeUtil.crName(jobId))
                                               .get();
        computerJob.getSpec().setMasterCpu(Quantity.parse("2"));
        this.operation.createOrReplace(computerJob);

        UnitTestBase.sleep(1000L);

        this.driver.cancelJob(jobId, params);

        UnitTestBase.sleep(1000L);

        future.cancel(true);
    }

    @Test
    public void testPullImageError() {
        Map<String, String> params = new HashMap<>();
        this.updateOptions(KubeDriverOptions.IMAGE_REPOSITORY_URL.name(),
                           "xxx");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.FAILED);
        Mockito.verify(jobObserver, Mockito.timeout(30000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        String diagnostics = this.driver.diagnostics(jobId, params);
        Assert.assertContains("ImagePullBackOff", diagnostics);

        future.cancel(true);
    }

    @Test
    public void testGetResourceListWithLabels() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.WORKER_INSTANCES.name(), "1");
        params.put(ComputerOptions.TRANSPORT_SERVER_PORT.name(), "0");
        params.put(ComputerOptions.RPC_SERVER_PORT_NAME, "0");
        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        List<AbstractController<?>> controllers = Whitebox.getInternalState(
                                                  this.entrypoint,
                                                  "controllers");
        AbstractController<?> abstractController = controllers.get(0);
        List<Pod> pods = Whitebox.invoke(AbstractController.class,
                                         new Class[]{String.class, Class.class,
                                                     Map.class},
                                         "getResourceListWithLabels",
                                         abstractController,
                                         this.namespace, Pod.class,
                                         new HashMap<String, String>());
        Assert.assertNotEquals(0, pods.size());

        future.cancel(true);
    }

    @Test
    public void testMountConfigMapAndSecret() {
        String dataBase64 = Base64.getEncoder()
                                  .encodeToString("test123\ntest".getBytes());

        String configMapName = "config-map-test";
        ConfigMap configMap = new ConfigMapBuilder()
                              .withNewMetadata()
                              .withNamespace(this.namespace)
                              .withName(configMapName)
                              .endMetadata()
                              .addToData("1.txt", "test123\ntest")
                              .build();
        this.kubeClient.configMaps().createOrReplace(configMap);

        String secretName = "secret-test";
        Secret secret = new SecretBuilder().withNewMetadata()
                        .withNamespace(this.namespace)
                        .withName(secretName)
                        .endMetadata()
                        .withType("Opaque")
                        .addToData("2.txt", dataBase64)
                        .addToData("3.txt", dataBase64)
                        .build();
        this.kubeClient.secrets().createOrReplace(secret);

        ArrayList<String> args = Lists.newArrayList(
                                 "cat /opt/configmap123/1.txt && " +
                                 "cat /opt/secret123/2.txt &&" +
                                 "cat /opt/secret123/3.txt");
        super.updateOptions(KubeSpecOptions.MASTER_ARGS.name(), args);
        super.updateOptions(KubeSpecOptions.WORKER_ARGS.name(), args);
        Object defaultSpec = Whitebox.invoke(KubernetesDriver.class,
                                             "defaultSpec", this.driver);
        Whitebox.setInternalState(this.driver, "defaultSpec", defaultSpec);

        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.CONFIG_MAP_PATHS.name(),
                   String.format("[%s:/opt/configmap123]",
                                 configMapName));
        params.put(KubeSpecOptions.SECRET_PATHS.name(),
                   String.format("[%s:/opt/secret123]", secretName));

        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(150000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.SUCCEEDED);
        Mockito.verify(jobObserver, Mockito.timeout(150000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));

        future.cancel(true);
    }

    @Test
    public void testMountConfigMapWithFailed() {
        Map<String, String> params = new HashMap<>();
        params.put(KubeSpecOptions.CONFIG_MAP_PATHS.name(),
                   "[test-config:/opt/configmap123]");

        String jobId = this.driver.submitJob(ALGORITHM_NAME, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);

        CompletableFuture<Void> future = this.driver.waitJobAsync(jobId, params,
                                                                  jobObserver);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(150000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));


        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.FAILED);
        Mockito.verify(jobObserver, Mockito.timeout(250000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));
        future.cancel(true);
    }
}
