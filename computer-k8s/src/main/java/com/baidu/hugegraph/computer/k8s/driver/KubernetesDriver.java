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

package com.baidu.hugegraph.computer.k8s.driver;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.ComputerDriver;
import com.baidu.hugegraph.computer.driver.DefaultJobState;
import com.baidu.hugegraph.computer.driver.JobObserver;
import com.baidu.hugegraph.computer.driver.JobState;
import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.computer.driver.SuperstepStat;
import com.baidu.hugegraph.computer.k8s.config.KubeDriverOptions;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class KubernetesDriver implements ComputerDriver {

    private static final Logger LOG = Log.logger(KubernetesDriver.class);

    private final HugeConfig conf;
    private final String namespace;
    private final KubernetesClient kubeClient;
    private final MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
            Resource<HugeGraphComputerJob>> operation;
    private volatile Watch watch;
    private final MutableBoolean watchActive;
    private final Map<String, Pair<CompletableFuture<Void>, JobObserver>> waits;

    public KubernetesDriver(HugeConfig conf) {
        this.conf = conf;
        this.namespace = this.conf.get(KubeDriverOptions.NAMESPACE);
        this.kubeClient = new DefaultKubernetesClient();
        this.operation = this.kubeClient.customResources(
                         HugeGraphComputerJob.class,
                         HugeGraphComputerJobList.class);

        this.watch = this.initWatch();
        this.watchActive = new MutableBoolean(true);
        this.waits = new ConcurrentHashMap<>();
    }

    @Override
    public void uploadAlgorithmJar(String algorithmName, InputStream input) {
        // TODO: implement
    }

    @Override
    public String submitJob(String algorithmName, Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public void cancelJob(String jobId, Map<String, String> params) {
        Boolean delete = this.operation.inNamespace(this.namespace)
                                       .withName(KubeUtil.crName(jobId))
                                       .delete();
        System.out.println(delete);
    }

    private Watch initWatch() {
        return this.operation.inNamespace(this.namespace)
                             .watch(new Watcher<HugeGraphComputerJob>() {
            @Override
            public void eventReceived(Action action,
                                      HugeGraphComputerJob computerJob) {
                if (computerJob == null) {
                    return;
                }
                String jobId = computerJob.getSpec().getJobId();
                if (StringUtils.isBlank(jobId)) {
                    return;
                }
                Pair<CompletableFuture<Void>, JobObserver> pair =
                        KubernetesDriver.this.waits.get(jobId);

                if (pair != null) {
                    CompletableFuture<?> future = pair.getLeft();
                    JobObserver observer = pair.getRight();
                    if (action == Action.ERROR) {
                        return;
                    }
                    JobState jobState = KubernetesDriver.this
                            .buildJobState(computerJob);
                    observer.onJobStateChanged(jobState);
                    if (JobStatus.finished(jobState.jobStatus())) {
                        future.complete(null);
                        KubernetesDriver.this.cancelWait(jobId);
                    }
                }
            }

            @Override
            public void onClose(WatcherException cause) {
                if (cause.isShouldRetry()) {
                    return;
                }

                for (Pair<CompletableFuture<Void>, JobObserver> pair :
                     KubernetesDriver.this.waits.values()) {
                    if (pair != null) {
                        CompletableFuture<Void> future = pair.getLeft();
                        future.completeExceptionally(cause);
                    }
                }
                synchronized (KubernetesDriver.this.watchActive) {
                    KubernetesDriver.this.waits.clear();
                    Watch watch = KubernetesDriver.this.watch;
                    if (watch != null) {
                        watch.close();
                    }
                    KubernetesDriver.this.watchActive.setFalse();
                }
            }
        });
    }

    @Override
    public void waitJob(String jobId, Map<String, String> params,
                        JobObserver observer) {
        JobState jobState = this.jobState(jobId, params);
        if (jobState == null) {
            LOG.warn("Unable to fetch Job: {}, it may have been deleted",
                     jobId);
            return;
        } else {
            observer.onJobStateChanged(jobState);
        }

        CompletableFuture<Void> future = null;
        synchronized (this.watchActive) {
            if (!this.watchActive.getValue()) {
                this.watch = this.initWatch();
                this.watchActive.setTrue();
            } else {
                future = new CompletableFuture<>();
                this.waits.put(jobId, Pair.of(future, observer));
            }
        }

        try {
            if (future != null) {
                future.get();
            }
        } catch (Throwable e) {
            this.cancelWait(jobId);
            throw KubernetesClientException.launderThrowable(e);
        }
    }

    @Override
    public JobState jobState(String jobId, Map<String, String> params) {
        HugeGraphComputerJob computerJob = this.operation
                .inNamespace(this.namespace)
                .withName(KubeUtil.crName(jobId))
                .get();
        if (computerJob == null) {
            return null;
        }
        return this.buildJobState(computerJob);
    }

    @Override
    public List<SuperstepStat> superstepStats(String jobId,
                                              Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public String diagnostics(String jobId, Map<String, String> params) {
        String crName = KubeUtil.crName(jobId);
        String eventName = KubeUtil.failedEventName(crName);
        Event event = this.kubeClient.v1().events()
                                     .inNamespace(this.namespace)
                                     .withName(eventName)
                                     .get();
        if (event == null) {
            return null;
        }
        return event.getMessage();
    }

    @Override
    public String log(String jobId, int containerId, long offset, long length,
                      Map<String, String> params) {
        // TODO: implement
        return null;
    }

    public void close() {
        if (this.watch != null) {
            this.watch.close();
        }

        if (this.kubeClient != null) {
            this.kubeClient.close();
        }
    }

    private void cancelWait(String jobId) {
        Pair<CompletableFuture<Void>, JobObserver> pair =
        this.waits.remove(jobId);
        if (pair != null) {
            CompletableFuture<Void> future = pair.getLeft();
            future.cancel(true);
        }
    }

    private JobState buildJobState(HugeGraphComputerJob computerJob) {
        E.checkNotNull(computerJob, "computerJob");
        ComputerJobStatus status = computerJob.getStatus();
        if (status == null || status.getJobStatus() == null) {
            return new DefaultJobState().jobStatus(JobStatus.INITIALIZING);
        }
        JobStatus jobStatus = JobStatus.valueOf(status.getJobStatus());
        return new DefaultJobState().jobStatus(jobStatus);
    }
}
