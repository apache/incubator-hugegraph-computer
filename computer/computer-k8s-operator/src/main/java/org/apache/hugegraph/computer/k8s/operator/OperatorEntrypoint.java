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

package org.apache.hugegraph.computer.k8s.operator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.k8s.Constants;
import org.apache.hugegraph.computer.k8s.operator.common.AbstractController;
import org.apache.hugegraph.computer.k8s.operator.config.OperatorOptions;
import org.apache.hugegraph.computer.k8s.operator.controller.ComputerJobController;
import org.apache.hugegraph.computer.k8s.util.KubeUtil;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;

import com.sun.net.httpserver.HttpServer;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.utils.Utils;

/**
 * The OperatorEntrypoint class is the main entry point for the Kubernetes operator.
 * It sets up the Kubernetes client, registers controllers, and starts the HTTP server for
 * health checks.
 */
public class OperatorEntrypoint {

    private static final Logger LOG = Log.logger(OperatorEntrypoint.class);

    private final HugeConfig config;
    private final List<AbstractController<?>> controllers;
    private NamespacedKubernetesClient kubeClient;
    private SharedInformerFactory informerFactory;
    private ExecutorService controllerPool;
    private HttpServer httpServer;

    public static void main(String[] args) {
        OperatorEntrypoint operatorEntrypoint = new OperatorEntrypoint();
        Runtime.getRuntime().addShutdownHook(new Thread(operatorEntrypoint::shutdown));
        operatorEntrypoint.start();
    }

    static {
        OptionSpace.register("computer-k8s-operator",
                             "org.apache.hugegraph.computer.k8s.operator.config.OperatorOptions"
        );
    }

    public OperatorEntrypoint() {
        this.config = this.configFromSysPropsOrEnvVars();
        this.controllers = new ArrayList<>();

        // Set time zone from config
        String timezone = this.config.get(OperatorOptions.TIMEZONE);
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }

    public void start() {
        try {
            this.kubeClient = new DefaultKubernetesClient();
            String watchNamespace = this.config.get(OperatorOptions.WATCH_NAMESPACE);
            if (!Objects.equals(watchNamespace, Constants.ALL_NAMESPACE)) {
                this.createNamespace(watchNamespace);
                this.kubeClient = this.kubeClient.inNamespace(watchNamespace);
            }
            this.informerFactory = this.kubeClient.informers();
            LOG.info("Watch namespace: " + watchNamespace);

            this.addHealthCheck();
            this.registerControllers();

            this.informerFactory.startAllRegisteredInformers();
            this.informerFactory.addSharedInformerEventListener(exception -> {
                LOG.error("Informer event listener exception occurred", exception);
                OperatorEntrypoint.this.shutdown();
            });

            // Start all controllers
            this.controllerPool = ExecutorUtil.newFixedThreadPool(this.controllers.size(),
                                                                  "controllers-%d");
            CountDownLatch latch = new CountDownLatch(this.controllers.size());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (AbstractController<?> controller : this.controllers) {
                futures.add(CompletableFuture.runAsync(() -> {
                    controller.run(latch);
                }, this.controllerPool));
            }

            // Block until controller startup is complete
            CompletableFuture.runAsync(() -> {
                try {
                    latch.await();
                    this.addReadyCheck();
                    LOG.info("The Operator has been ready");
                } catch (Throwable e) {
                    LOG.error("Failed to set up ready check");
                    OperatorEntrypoint.this.shutdown();
                }
            });

            CompletableFuture.anyOf(futures.toArray(new CompletableFuture[]{})).get();
        } catch (Throwable throwable) {
            LOG.error("Failed to start Operator: ", throwable);
        } finally {
            this.shutdown();
        }
    }

    public synchronized void shutdown() {
        LOG.info("The Operator shutdown...");

        Iterator<AbstractController<?>> iterator = this.controllers.iterator();
        while (iterator.hasNext()) {
            AbstractController<?> controller = iterator.next();
            if (controller != null) {
                controller.shutdown();
            }
            iterator.remove();
        }

        if (this.controllerPool != null) {
            this.controllerPool.shutdown();
            this.controllerPool = null;
        }

        if (this.informerFactory != null) {
            this.informerFactory.stopAllRegisteredInformers();
            this.informerFactory = null;
        }

        if (this.kubeClient != null) {
            this.kubeClient.close();
            this.kubeClient = null;
        }

        if (this.httpServer != null) {
            this.httpServer.stop(0);
            this.httpServer = null;
        }

        LogManager.shutdown();
    }

    private HugeConfig configFromSysPropsOrEnvVars() {
        Map<String, String> confMap = new HashMap<>();
        Set<String> keys = OperatorOptions.instance().options().keySet();
        for (String key : keys) {
            String envKey = key.substring(Constants.K8S_SPEC_PREFIX.length())
                               .toUpperCase(Locale.ROOT);
            String value = Utils.getSystemPropertyOrEnvVar(envKey);
            if (StringUtils.isNotBlank(value)) {
                confMap.put(key, value);
            }
        }
        MapConfiguration configuration = new MapConfiguration(confMap);
        return new HugeConfig(configuration);
    }

    private void registerControllers() {
        ComputerJobController jobController = new ComputerJobController(this.config,
                                                                        this.kubeClient);
        this.registerController(jobController, ConfigMap.class, Job.class, Pod.class);
    }

    @SafeVarargs
    private void registerController(AbstractController<?> controller,
                                    Class<? extends HasMetadata>... ownsClass) {
        controller.register(this.informerFactory, ownsClass);
        this.controllers.add(controller);
    }

    private void addHealthCheck() throws IOException {
        Integer probePort = this.config.get(OperatorOptions.PROBE_PORT);
        InetSocketAddress address = new InetSocketAddress(probePort);
        Integer probeBacklog = this.config.get(OperatorOptions.PROBE_BACKLOG);
        this.httpServer = HttpServer.create(address, probeBacklog);
        this.httpServer.createContext("/health", httpExchange -> {
            byte[] bytes = "ALL GOOD!".getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bytes.length);
            OutputStream responseBody = httpExchange.getResponseBody();
            responseBody.write(bytes);
            responseBody.close();
        });
        this.httpServer.start();
    }

    private void addReadyCheck() {
        this.httpServer.createContext("/ready", httpExchange -> {
            byte[] bytes = "ALL Ready!".getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bytes.length);
            OutputStream responseBody = httpExchange.getResponseBody();
            responseBody.write(bytes);
            responseBody.close();
        });
    }

    private void createNamespace(String namespace) {
        NamespaceBuilder builder = new NamespaceBuilder().withNewMetadata()
                                                         .withName(namespace)
                                                         .endMetadata();
        KubeUtil.ignoreExists(() -> {
            return this.kubeClient.namespaces().create(builder.build());
        });
    }
}
