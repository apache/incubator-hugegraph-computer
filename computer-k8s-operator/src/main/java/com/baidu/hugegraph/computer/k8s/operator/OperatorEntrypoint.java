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

package com.baidu.hugegraph.computer.k8s.operator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.k8s.operator.controller.AbstractController;
import com.baidu.hugegraph.computer.k8s.operator.controller.ComputerJobController;
import com.baidu.hugegraph.util.Log;
import com.sun.net.httpserver.HttpServer;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

public class OperatorEntrypoint {

    private static final Logger LOG = Log.logger(OperatorEntrypoint.class);
    private static final int RESYNC_PERIOD = 10 * 60 * 1000;
    private static final int PROBE_PORT = 8081;
    private static final int PROBE_BACK_LOG = 50;

    private final KubernetesClient kubeClient;
    private final SharedInformerFactory informerFactory;
    private final List<AbstractController<?>> controllers;
    private HttpServer httpServer;

    public static void main(String[] args) {
        OperatorEntrypoint operatorEntrypoint = new OperatorEntrypoint();
        operatorEntrypoint.start();
        Runtime.getRuntime().addShutdownHook(
                new Thread(operatorEntrypoint::shutdown));
    }

    public OperatorEntrypoint() {
        this.kubeClient = new DefaultKubernetesClient();
        this.informerFactory = this.kubeClient.informers();
        this.controllers = new ArrayList<>();
    }

    public void start() {
        try {
            String namespace = this.kubeClient.getNamespace();
            if (namespace == null) {
                LOG.warn("No namespace found via config, assuming default.");
                namespace = "default";
            }
            LOG.info("Using namespace: " + namespace);

            // Registered controller
            this.registerControllers();

            this.informerFactory.startAllRegisteredInformers();
            this.informerFactory.addSharedInformerEventListener(exception -> {
                LOG.error("Informer Exception occurred, but caught", exception);
            });

            CountDownLatch readyLatch = new CountDownLatch(this.controllers.size());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (AbstractController<?> controller : this.controllers) {
                futures.add(CompletableFuture.runAsync(() -> {
                    controller.run(readyLatch);
                }));
            }

            this.addHealthCheck();

            CompletableFuture.runAsync(() -> {
                try {
                    readyLatch.await();
                    this.addReadyCheck();
                } catch (InterruptedException | IOException e) {
                    System.exit(1);
                }
            });

            CompletableFuture.anyOf(futures.toArray(new CompletableFuture[]{}))
                             .get();
        } catch (Throwable throwable) {
            LOG.error("Failed to start Operator: ", throwable);
        } finally {
            this.shutdown();
        }
    }

    public void shutdown() {
        LOG.info("The Operator shutdown...");

        if (this.controllers != null) {
            Iterator<AbstractController<?>> iterator =
                                            this.controllers.iterator();
            while (iterator.hasNext()) {
                AbstractController<?> controller = iterator.next();
                if (controller != null) {
                    controller.shutdown();
                }
                iterator.remove();
            }
        }

        if (this.informerFactory != null) {
            this.informerFactory.stopAllRegisteredInformers();
        }

        if (this.kubeClient != null) {
            this.kubeClient.close();
        }

        if (this.httpServer != null) {
            this.httpServer.stop(0);
            this.httpServer = null;
        }
    }

    private void registerControllers() {
        this.registerController(new ComputerJobController(this.kubeClient),
                                Deployment.class, ConfigMap.class);
    }

    @SafeVarargs
    private final void registerController(
                       AbstractController<?> controller,
                       Class<? extends HasMetadata>...ownsClass) {
        controller.register(this.informerFactory, RESYNC_PERIOD, ownsClass);
        this.controllers.add(controller);
    }

    private void addHealthCheck() throws IOException {
        InetSocketAddress address = new InetSocketAddress(PROBE_PORT);
        this.httpServer = HttpServer.create(address, PROBE_BACK_LOG);
        this.httpServer.createContext("/health", httpExchange -> {
            byte[] bytes = "ALL GOOD!".getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length);
            OutputStream responseBody = httpExchange.getResponseBody();
            responseBody.write(bytes);
            responseBody.close();
        });
        this.httpServer.setExecutor(ForkJoinPool.commonPool());
        this.httpServer.start();
    }

    private void addReadyCheck() throws IOException {
        this.httpServer.createContext("/ready", httpExchange -> {
            byte[] bytes = "ALL Ready!".getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length);
            OutputStream responseBody = httpExchange.getResponseBody();
            responseBody.write(bytes);
            responseBody.close();
        });
    }
}
