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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
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
import com.baidu.hugegraph.computer.driver.config.ComputerOptions;
import com.baidu.hugegraph.computer.driver.config.NoDefaultConfigOption;
import com.baidu.hugegraph.computer.k8s.config.KubeDriverOptions;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.IOHelpers;

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
    private final Map<String, String> defaultConf;
    private final Map<String, Object> defaultSpec;

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
        this.defaultConf = this.defaultComputerConf();
        this.defaultSpec = this.defaultSpec();
    }

    @Override
    public void uploadAlgorithmJar(String algorithmName, InputStream input) {
        File tempFile = null;
        try {
            final String suffix = ".jar";
            tempFile = File.createTempFile(UUID.randomUUID().toString(),
                                           suffix);
            FileUtils.copyInputStreamToFile(input, tempFile);

            String shell = this.conf.get(KubeDriverOptions.BUILD_IMAGE_SHELL);
            String registry = this.conf.get(
                                   KubeDriverOptions.IMAGE_REPOSITORY_REGISTRY)
                                  .trim();
            String username = this.conf.get(
                                   KubeDriverOptions.IMAGE_REPOSITORY_USERNAME);
            String password = this.conf.get(
                                   KubeDriverOptions.IMAGE_REPOSITORY_PASSWORD);
            String imageUrl = this.buildImageUrl(algorithmName);

            String template = "sh %s -r %s -u %s -p %s -j %s -i %s";
            String command = String.format(template, shell, registry,
                                           username, password,
                                           tempFile.getAbsolutePath(),
                                           imageUrl);

            Process process = Runtime.getRuntime().exec(command);
            int code = process.waitFor();
            if (code != 0) {
                InputStream errorStream = process.getErrorStream();
                String errorInfo = IOHelpers.readFully(errorStream);
                throw new RuntimeException(
                          "Failed to upload algorithm Jar: " + errorInfo);
            }
        } catch (Throwable exception) {
            throw new RuntimeException(exception);
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
    }

    @Override
    public String submitJob(String algorithmName, Map<String, String> params) {
        HugeGraphComputerJob computerJob = new HugeGraphComputerJob();
        String jobId = KubeUtil.genJobId(algorithmName);
        String crName = KubeUtil.crName(jobId);

        ObjectMeta meta = new ObjectMetaBuilder()
                .withNamespace(this.namespace)
                .withName(crName)
                .build();
        computerJob.setMetadata(meta);

        Map<String, Object> specMap = new HashMap<>(this.defaultSpec);
        KubeDriverOptions.ALLOW_USER_SETTINGS.forEach((key, typeOption) -> {
            String value = params.get(key);
            if (StringUtils.isNotBlank(value)) {
                Object parsed = typeOption.parseConvert(value);
                String specKey = KubeUtil.covertSpecKey(key);
                specMap.put(specKey, parsed);
            }
        });

        ComputerJobSpec spec = HugeGraphComputerJob.mapToSpec(specMap);
        Map<String, String> computerConf = this.computerConf(params);
        spec.withAlgorithmName(algorithmName)
            .withJobId(jobId)
            .withImage(this.buildImageUrl(algorithmName))
            .withComputerConf(computerConf);
        computerJob.setSpec(spec);

        this.operation.inNamespace(this.namespace).createOrReplace(computerJob);
        return jobId;
    }

    @Override
    public void cancelJob(String jobId, Map<String, String> params) {
        Boolean delete = this.operation.inNamespace(this.namespace)
                                       .withName(KubeUtil.crName(jobId))
                                       .delete();
        E.checkState(delete, "Failed to cancel Job, jobId: ", jobId);
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

    private void cancelWait(String jobId) {
        Pair<CompletableFuture<Void>, JobObserver> pair =
                this.waits.remove(jobId);
        if (pair != null) {
            CompletableFuture<Void> future = pair.getLeft();
            future.cancel(true);
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

    private JobState buildJobState(HugeGraphComputerJob computerJob) {
        E.checkNotNull(computerJob, "computerJob");
        ComputerJobStatus status = computerJob.getStatus();
        if (status == null || status.getJobStatus() == null) {
            return new DefaultJobState().jobStatus(JobStatus.INITIALIZING);
        }
        JobStatus jobStatus = JobStatus.valueOf(status.getJobStatus());
        return new DefaultJobState().jobStatus(jobStatus);
    }

    private String buildImageUrl(String algorithmName) {
        String repository = this.conf.get(
                                 KubeDriverOptions.IMAGE_REPOSITORY_URL);
        return KubeUtil.imageName(repository, algorithmName, null);
    }

    private Map<String, String> computerConf(Map<String, String> params) {
        Map<String, String> computerConf = new HashMap<>(this.defaultConf);
        Map<String, TypedOption<?, ?>> allOptions = ComputerOptions.instance()
                                                                   .options();
        params.forEach((k, v) -> {
            if (k != null && v != null) {
                if (!ComputerOptions.PROHIBIT_USER_SETTINGS.contains(k)) {
                    NoDefaultConfigOption<?> typedOption =
                                             (NoDefaultConfigOption<?>)
                                             allOptions.get(k);
                    if (typedOption != null) {
                        typedOption.checkVal(v);
                    }
                    computerConf.put(k, v);
                }
            }
        });
        return computerConf;
    }

    private Map<String, String> defaultComputerConf() {
        Map<String, String> defaultConf = new HashMap<>();
        Map<String, TypedOption<?, ?>> allOptions =
                                       ComputerOptions.instance().options();
        Set<String> keys = allOptions.keySet();
        for (String key : keys) {
            String value = this.conf.getString(key);
            if (value != null) {
                defaultConf.put(key, value);
            }
        }
        return Collections.unmodifiableMap(defaultConf);
    }

    private Map<String, Object> defaultSpec() {
        Map<String, Object> defaultSpec = new HashMap<>();
        Map<String, TypedOption<?, ?>> allOptions =
                                       KubeDriverOptions.instance().options();
        Collection<TypedOption<?, ?>> typedOptions = allOptions.values();
        for (TypedOption<?, ?> typedOption : typedOptions) {
            Object value = this.conf.get(typedOption);
            if (value != null) {
                String specKey = KubeUtil.covertSpecKey(typedOption.name());
                defaultSpec.put(specKey, value);
            }
        }
        ComputerJobSpec spec = HugeGraphComputerJob.mapToSpec(defaultSpec);

        // Add pullSecrets
        List<String> secretNames = this.conf.get(
                                        KubeDriverOptions.PULL_SECRET_NAMES);
        if (CollectionUtils.isNotEmpty(secretNames)) {
            List<LocalObjectReference> secrets = new ArrayList<>();
            for (String name : secretNames) {
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                secrets.add(new LocalObjectReference(name));
            }
            if (CollectionUtils.isNotEmpty(secrets)) {
                spec.withPullSecrets(secrets);
            }
        }

        Map<String, Object> specMap = HugeGraphComputerJob.specToMap(spec);
        return Collections.unmodifiableMap(specMap);
    }
}
