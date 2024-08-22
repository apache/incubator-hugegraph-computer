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

package org.apache.hugegraph.computer.k8s.driver;

import static org.apache.hugegraph.computer.core.config.ComputerOptions.COMPUTER_PROHIBIT_USER_OPTIONS;
import static org.apache.hugegraph.computer.core.config.ComputerOptions.COMPUTER_REQUIRED_USER_OPTIONS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.driver.ComputerDriver;
import org.apache.hugegraph.computer.driver.ComputerDriverException;
import org.apache.hugegraph.computer.driver.DefaultJobState;
import org.apache.hugegraph.computer.driver.JobObserver;
import org.apache.hugegraph.computer.driver.JobState;
import org.apache.hugegraph.computer.driver.JobStatus;
import org.apache.hugegraph.computer.driver.SuperstepStat;
import org.apache.hugegraph.computer.k8s.Constants;
import org.apache.hugegraph.computer.k8s.config.KubeDriverOptions;
import org.apache.hugegraph.computer.k8s.config.KubeSpecOptions;
import org.apache.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import org.apache.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import org.apache.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import org.apache.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import org.apache.hugegraph.computer.k8s.util.KubeUtil;
import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.IOHelpers;
import io.fabric8.kubernetes.client.utils.URLUtils;

public class KubernetesDriver implements ComputerDriver {

    private static final Logger LOG = Log.logger(KubernetesDriver.class);

    private final HugeConfig conf;
    private final String namespace;
    private final NamespacedKubernetesClient kubeClient;
    private final MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
                                 Resource<HugeGraphComputerJob>> operation;
    private volatile Watch watch;
    private final MutableBoolean watchActive;
    private final Map<String, Pair<CompletableFuture<Void>, JobObserver>> waits;
    private final Map<String, Object> defaultSpec;
    private final Map<String, String> defaultConf;

    private final String bashPath;
    private final String jarFileDir;
    private final String registry;
    private final String username;
    private final String password;
    private final Boolean enableInternalAlgorithm;
    private final List<String> internalAlgorithms;
    private final String internalAlgorithmImageUrl;
    private final String frameworkImageUrl;

    private static final String DEFAULT_PUSH_BASH_PATH = "/docker_push.sh";
    private static final String BUILD_IMAGE_FUNC = "build_image";
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    public KubernetesDriver(HugeConfig conf) {
        this(conf, createKubeClient(conf));
    }

    public KubernetesDriver(HugeConfig conf,
                            NamespacedKubernetesClient kubeClient) {
        this.conf = conf;
        this.namespace = this.conf.get(KubeDriverOptions.NAMESPACE);
        this.kubeClient = kubeClient.inNamespace(this.namespace);
        this.operation = this.kubeClient.customResources(
                         HugeGraphComputerJob.class,
                         HugeGraphComputerJobList.class);
        this.watch = this.initWatch();
        this.watchActive = new MutableBoolean(true);
        this.waits = new ConcurrentHashMap<>();
        this.defaultSpec = this.defaultSpec();
        this.defaultConf = this.defaultComputerConf();

        this.bashPath = this.conf.get(KubeDriverOptions.BUILD_IMAGE_BASH_PATH);
        this.jarFileDir = this.conf.get(KubeDriverOptions.JAR_FILE_DIR);
        this.registry = this.conf.get(
                KubeDriverOptions.IMAGE_REPOSITORY_REGISTRY).trim();
        this.username = this.conf.get(
                KubeDriverOptions.IMAGE_REPOSITORY_USERNAME);
        this.password = this.conf.get(
                KubeDriverOptions.IMAGE_REPOSITORY_PASSWORD);
        this.enableInternalAlgorithm = this.conf.get(
                KubeDriverOptions.ENABLE_INTERNAL_ALGORITHM);
        this.internalAlgorithms = this.conf.get(
                KubeDriverOptions.INTERNAL_ALGORITHMS);
        this.internalAlgorithmImageUrl = this.conf.get(
                KubeDriverOptions.INTERNAL_ALGORITHM_IMAGE_URL);
        this.frameworkImageUrl = this.conf.get(
                KubeDriverOptions.FRAMEWORK_IMAGE_URL);
    }

    private static NamespacedKubernetesClient createKubeClient(
                                              HugeConfig conf) {
        String kubeConfig = conf.get(KubeDriverOptions.KUBE_CONFIG);
        Config config;
        try {
            File file = new File(kubeConfig);
            @SuppressWarnings("deprecation")
            String kubeConfigContents = FileUtils.readFileToString(file);
            config = Config.fromKubeconfig(kubeConfigContents);
        } catch (IOException e) {
            throw new ComputerDriverException("Failed to read KubeConfig: %s",
                                              e, kubeConfig);
        }
        return new DefaultKubernetesClient(config);
    }

    @Override
    public void uploadAlgorithmJar(String algorithmName, InputStream input) {
        File tempFile = null;
        try {
            Path path = Files.createDirectories(
                        Paths.get(TMP_DIR, UUID.randomUUID().toString()));
            tempFile = File.createTempFile("userAlgorithm", ".jar",
                                           path.toFile());
            FileUtils.copyInputStreamToFile(input, tempFile);

            InputStream bashStream;
            if (StringUtils.isBlank(this.bashPath)) {
                bashStream = this.getClass()
                                 .getResourceAsStream(DEFAULT_PUSH_BASH_PATH);
            } else {
                bashStream = new FileInputStream(this.bashPath);
            }
            String bashAsStr = IOHelpers.readFully(bashStream);

            StringBuilder builder = new StringBuilder();
            builder.append(BUILD_IMAGE_FUNC);
            if (StringUtils.isNotBlank(this.registry)) {
                builder.append(" -r ").append(this.registry);
            }
            if (StringUtils.isNotBlank(this.username)) {
                builder.append(" -u ").append(this.username);
            }
            if (StringUtils.isNotBlank(this.password)) {
                builder.append(" -p ").append(this.password);
            }
            builder.append(" -s ").append(tempFile.getAbsolutePath());
            String jarFile = this.buildJarFile(this.jarFileDir, algorithmName);
            builder.append(" -j ").append(jarFile);
            String imageUrl = this.buildImageUrl(algorithmName);
            builder.append(" -i ").append(imageUrl);
            builder.append(" -f ").append(this.frameworkImageUrl);
            String args = builder.toString();
            String[] command = {"bash", "-c", bashAsStr + "\n" + args};

            Process process = Runtime.getRuntime().exec(command);
            int code = process.waitFor();
            if (code != 0) {
                InputStream errorStream = process.getErrorStream();
                String errorInfo = IOHelpers.readFully(errorStream);
                if (StringUtils.isBlank(errorInfo)) {
                    InputStream stdoutStream = process.getInputStream();
                    errorInfo = IOHelpers.readFully(stdoutStream);
                }
                throw new ComputerDriverException(errorInfo);
            }
        } catch (Throwable exception) {
            throw new ComputerDriverException("Failed to upload algorithm Jar",
                                              exception);
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
    }

    @Override
    public String submitJob(String algorithmName, Map<String, String> params) {
        HugeGraphComputerJob computerJob = new HugeGraphComputerJob();
        String jobId = KubeUtil.genJobId(algorithmName);
        String crName = KubeUtil.crName(jobId);

        ObjectMeta meta = new ObjectMetaBuilder().withNamespace(this.namespace)
                                                 .withName(crName)
                                                 .build();
        computerJob.setMetadata(meta);

        ComputerJobSpec spec = this.computerJobSpec(this.defaultSpec, params);

        Map<String, String> computerConf = this.computerConf(this.defaultConf,
                                                             params);
        this.checkComputerConf(computerConf, spec);

        spec.withAlgorithmName(algorithmName)
            .withJobId(jobId)
            .withComputerConf(computerConf);

        if (this.enableInternalAlgorithm &&
            this.internalAlgorithms.contains(algorithmName)) {
            spec.withImage(this.internalAlgorithmImageUrl);
        } else if (StringUtils.isNotBlank(spec.getRemoteJarUri())) {
            spec.withImage(this.frameworkImageUrl);
        } else {
            String imageUrl = this.buildImageUrl(algorithmName);
            String jarFileDir = this.conf.get(KubeDriverOptions.JAR_FILE_DIR);
            String jarFile = this.buildJarFile(jarFileDir, algorithmName);
            spec.withImage(imageUrl)
                .withJarFile(jarFile);
        }

        computerJob.setSpec(spec);

        this.operation.createOrReplace(computerJob);
        return jobId;
    }

    private void checkComputerConf(Map<String, String> computerConf,
                                   ComputerJobSpec spec) {
        @SuppressWarnings("unchecked")
        Collection<String> unSetOptions = CollectionUtils.removeAll(
                                          COMPUTER_REQUIRED_USER_OPTIONS,
                                          computerConf.keySet());
        E.checkArgument(unSetOptions.isEmpty(),
                        "The %s options can't be null", unSetOptions);

        int partitionsCount = Integer.parseInt(computerConf.getOrDefault(
                              ComputerOptions.JOB_PARTITIONS_COUNT.name(),
                              "1"));
        int workerInstances = spec.getWorkerInstances();
        E.checkArgument(partitionsCount >= workerInstances,
                        "The partitions count must be >= workers instances, " +
                        "but got %s < %s", partitionsCount, workerInstances);
    }

    @Override
    public boolean cancelJob(String jobId, Map<String, String> params) {
        return this.operation.withName(KubeUtil.crName(jobId))
                             .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                             .delete();
    }

    @Override
    public CompletableFuture<Void> waitJobAsync(String jobId,
                                                Map<String, String> params,
                                                JobObserver observer) {
        JobState jobState = this.jobState(jobId, params);
        if (jobState == null) {
            LOG.warn("Unable to fetch state of job '{}', it may have been " +
                     "deleted", jobId);
            return null;
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

        return future;
    }

    private Watch initWatch() {
        return this.operation.watch(new Watcher<HugeGraphComputerJob>() {
            @Override
            public void eventReceived(Action action,
                                      HugeGraphComputerJob computerJob) {
                if (computerJob == null) {
                    return;
                }

                if (action == Action.ERROR) {
                    return;
                }

                String jobId = computerJob.getSpec().getJobId();
                if (StringUtils.isBlank(jobId)) {
                    return;
                }

                Pair<CompletableFuture<Void>,
                     JobObserver> pair = KubernetesDriver.this.waits.get(jobId);

                if (pair != null) {
                    CompletableFuture<?> future = pair.getLeft();
                    JobObserver observer = pair.getRight();

                    @SuppressWarnings("resource")
                    KubernetesDriver driver = KubernetesDriver.this;
                    JobState jobState = driver.buildJobState(computerJob);

                    observer.onJobStateChanged(jobState);

                    if (JobStatus.finished(jobState.jobStatus())) {
                        future.complete(null);
                        driver.cancelWait(jobId);
                    }
                }
            }

            @Override
            public void onClose(WatcherException cause) {
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
        Pair<CompletableFuture<Void>, JobObserver> pair = this.waits.remove(jobId);
        if (pair != null) {
            CompletableFuture<Void> future = pair.getLeft();
            future.cancel(true);
        }
    }

    @Override
    public JobState jobState(String jobId, Map<String, String> params) {
        String crName = KubeUtil.crName(jobId);
        HugeGraphComputerJob computerJob = this.operation.withName(crName).get();
        if (computerJob == null) {
            return null;
        }
        return this.buildJobState(computerJob);
    }

    @Override
    public List<SuperstepStat> superstepStats(String jobId, Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public String diagnostics(String jobId, Map<String, String> params) {
        String crName = KubeUtil.crName(jobId);
        String eventName = KubeUtil.failedEventName(crName);
        Event event = this.kubeClient.v1().events()
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

    @Override
    public void close() {
        Iterator<Pair<CompletableFuture<Void>, JobObserver>> iterator =
        this.waits.values().iterator();
        while (iterator.hasNext()) {
            CompletableFuture<Void> future = iterator.next().getLeft();
            future.cancel(true);
            iterator.remove();
        }

        if (this.watch != null) {
            this.watch.close();
            this.watchActive.setFalse();
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
        return KubeUtil.imageUrl(repository, algorithmName, null);
    }

    private String buildJarFile(String jarFileDir, String algorithmName) {
        return URLUtils.join(jarFileDir, algorithmName + ".jar");
    }

    private Map<String, String> computerConf(Map<String, String> defaultConf,
                                             Map<String, String> params) {
        Map<String, String> computerConf = new HashMap<>(defaultConf);
        Map<String, TypedOption<?, ?>> allOptions = ComputerOptions.instance().options();
        params.forEach((k, v) -> {
            if (StringUtils.isNotBlank(k) && StringUtils.isNotBlank(v)) {
                if (!k.startsWith(Constants.K8S_SPEC_PREFIX) &&
                    !COMPUTER_PROHIBIT_USER_OPTIONS.contains(k)) {
                    ConfigOption<?> typedOption = (ConfigOption<?>) allOptions.get(k);
                    if (typedOption != null) {
                        // check value
                        typedOption.parseConvert(v);
                    }
                    computerConf.put(k, v);
                }
            }
        });
        return computerConf;
    }

    private Map<String, String> defaultComputerConf() {
        Map<String, String> defaultConf = new HashMap<>();

        Collection<TypedOption<?, ?>> options = ComputerOptions.instance().options().values();
        for (TypedOption<?, ?> typedOption : options) {
            Object value = this.conf.get(typedOption);
            String key = typedOption.name();
            if (value != null) {
                defaultConf.put(key, String.valueOf(value));
            } else {
                boolean required = ComputerOptions.REQUIRED_OPTIONS.contains(key);
                E.checkArgument(!required, "The %s option can't be null", key);
            }
        }
        return Collections.unmodifiableMap(defaultConf);
    }

    private ComputerJobSpec computerJobSpec(Map<String, Object> defaultSpec,
                                            Map<String, String> params) {
        Map<String, Object> specMap = new HashMap<>(defaultSpec);
        KubeSpecOptions.ALLOW_USER_SETTINGS.forEach((key, typeOption) -> {
            String value = params.get(key);
            if (StringUtils.isNotBlank(value)) {
                String specKey = KubeUtil.covertSpecKey(key);
                if (KubeSpecOptions.MAP_TYPE_CONFIGS.contains(typeOption)) {
                    if (StringUtils.isNotBlank(value)) {
                        Map<String, String> result = new HashMap<>();
                        List<String> values = (List<String>) typeOption.parseConvert(value);
                        for (String str : values) {
                            String[] pair = str.split(":", 2);
                            assert pair.length == 2;
                            result.put(pair[0], pair[1]);
                        }
                        specMap.put(specKey, result);
                    }
                } else {
                    Object parsed = typeOption.parseConvert(value);
                    specMap.put(specKey, parsed);
                }
            }
        });
        return HugeGraphComputerJob.mapToSpec(specMap);
    }

    private Map<String, Object> defaultSpec() {
        Map<String, Object> defaultSpec = new HashMap<>();

        Collection<TypedOption<?, ?>> options = KubeSpecOptions.instance().options().values();
        for (TypedOption<?, ?> typeOption : options) {
            Object value = this.conf.get(typeOption);
            if (value != null) {
                String specKey = KubeUtil.covertSpecKey(typeOption.name());
                if (KubeSpecOptions.MAP_TYPE_CONFIGS.contains(typeOption)) {
                    if (!Objects.equals(String.valueOf(value), "[]")) {
                        value = this.conf.getMap((ConfigListOption<String>) typeOption);
                        defaultSpec.put(specKey, value);
                    }
                } else {
                    defaultSpec.put(specKey, value);
                }
            }
        }
        ComputerJobSpec spec = HugeGraphComputerJob.mapToSpec(defaultSpec);

        // Add pullSecrets
        List<String> secretNames = this.conf.get(KubeDriverOptions.PULL_SECRET_NAMES);
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

        // Add log4j.xml
        String log4jXmlPath = this.conf.get(KubeDriverOptions.LOG4J_XML_PATH);
        if (StringUtils.isNotBlank(log4jXmlPath)) {
            try {
                File file = new File(log4jXmlPath);
                @SuppressWarnings("deprecation")
                String log4jXml = FileUtils.readFileToString(file);
                spec.withLog4jXml(log4jXml);
            } catch (IOException exception) {
                throw new ComputerDriverException(
                          "Failed to read log4j file for computer job",
                          exception);
            }
        }

        Map<String, Object> specMap = HugeGraphComputerJob.specToMap(spec);
        return Collections.unmodifiableMap(specMap);
    }
}
