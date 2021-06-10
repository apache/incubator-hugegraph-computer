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

package com.baidu.hugegraph.computer.k8s.operator.controller;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import jersey.repackaged.com.google.common.collect.Sets;

public abstract class AbstractController<T extends CustomResource<?, ?>> {

    private static final Logger LOG = Log.logger(AbstractController.class);

    private static final int WORK_QUEUE_CAPACITY = 1024;
    private static final Duration RECONCILE_PERIOD = Duration.ofSeconds(1L);
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration READY_CHECK_INTERNAL = Duration.ofSeconds(1);
    private static final int WORKER_COUNT =
            Runtime.getRuntime().availableProcessors();

    protected final String kind;
    protected final KubernetesClient kubeClient;
    private final BlockingQueue<Request> workQueue;
    private final ScheduledExecutorService executor;
    private final Class<T> crClass;
    private Set<Class<? extends HasMetadata>> ownsClassSet;
    private final Map<Class<? extends HasMetadata>,
            SharedIndexInformer<? extends HasMetadata>> informerMap;
    private final Duration readyTimeout;
    private final Duration readyCheckInternal;

    public AbstractController(KubernetesClient kubeClient) {
        this.crClass = this.crClass();
        this.kind = HasMetadata.getKind(this.crClass);
        this.kubeClient = kubeClient;
        this.workQueue = new ArrayBlockingQueue<>(WORK_QUEUE_CAPACITY);
        this.informerMap = new ConcurrentHashMap<>();
        this.readyTimeout = READY_TIMEOUT;
        this.readyCheckInternal = READY_CHECK_INTERNAL;
        this.executor = ExecutorUtil.newScheduledThreadPool(WORKER_COUNT,
                                                            "reconciler");
    }

    @SafeVarargs
    public final void register(SharedInformerFactory informerFactory,
                               long resyncPeriod,
                               Class<? extends HasMetadata>...ownsClass) {
        this.ownsClassSet = Sets.newHashSet(ownsClass);
        this.registerCREvent(informerFactory, resyncPeriod);
        this.registerOwnsEvent(informerFactory, resyncPeriod);
    }

    public void run(CountDownLatch readyLatch) {
        LOG.info("Starting the {}-controller...", this.kind);

        if (!this.hasSynced()) {
            LOG.error("Failed to start {}-controller: Timed out waiting " +
                      "for informer to be synced", this.kind);
            return;
        }
        CountDownLatch latch = new CountDownLatch(WORKER_COUNT);
        // Spawns worker threads for the controller.
        for (int i = 0; i < WORKER_COUNT; i++) {
            this.executor.scheduleWithFixedDelay(
                    () -> {
                        try {
                            this.worker();
                        } catch (Throwable e) {
                            LOG.error("Unexpected reconcile loop abortion", e);
                        } finally {
                            latch.countDown();
                        }
                    },
                    Duration.ZERO.toMillis(),
                    RECONCILE_PERIOD.toMillis(), TimeUnit.MILLISECONDS
            );
        }

        readyLatch.countDown();

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Aborting {}-controller.", this.kind, e);
        } finally {
            LOG.info("The {}-controller exited", this.kind);
        }
    }

    public void shutdown() {
        this.executor.shutdown();
        this.workQueue.clear();
    }

    protected abstract Result reconcile(Request request);

    protected abstract void onCRDelete(T cr, boolean deletedStateUnknown);

    private void worker() {
        while (!this.executor.isShutdown() &&
               !Thread.currentThread().isInterrupted()) {
            LOG.info("Trying to get item from work queue of {}-controller",
                     this.kind);
            Request request = null;
            try {
                request = this.workQueue.take();
            } catch (InterruptedException e) {
                LOG.error("The {}-controller worker interrupted...",
                          this.kind, e);
            }
            if (request == null) {
                LOG.info("The {}-controller worker exiting...", this.kind);
                return;
            }
            Result result;
            try {
                result = this.reconcile(request);
            } catch (Throwable e) {
                result = Result.REQUEUE;
            }
            if (result.requeue()) {
                this.workQueue.add(request);
            }
        }
    }

    protected void registerCREvent(SharedInformerFactory informerFactory,
                                   long resyncPeriod) {
        SharedIndexInformer<T> crInformer =
        informerFactory.sharedIndexInformerForCustomResource(this.crClass,
                                                             resyncPeriod);

        crInformer.addEventHandler(new ResourceEventHandler<T>() {
            @Override
            public void onAdd(T cr) {
                Request request = Request.parseRequestByCR(cr);
                AbstractController.this.enqueueRequest(request);
            }

            @Override
            public void onUpdate(T oldCr, T newCr) {
                Request request = Request.parseRequestByCR(newCr);
                AbstractController.this.enqueueRequest(request);
            }

            @Override
            public void onDelete(T cr, boolean deletedStateUnknown) {
                AbstractController.this.onCRDelete(cr, deletedStateUnknown);
                Request request = Request.parseRequestByCR(cr);
                AbstractController.this.enqueueRequest(request);
            }
        });
        this.informerMap.put(this.crClass, crInformer);
    }

    protected void registerOwnsEvent(SharedInformerFactory informerFactory,
                                     long resyncPeriod) {
        this.ownsClassSet.forEach(ownsClass -> {
            @SuppressWarnings("unchecked")
            SharedIndexInformer<HasMetadata> informer =
                    informerFactory.sharedIndexInformerFor(
                    (Class<HasMetadata>) ownsClass, resyncPeriod);

            informer.addEventHandler(new ResourceEventHandler<HasMetadata>() {
                @Override
                public void onAdd(HasMetadata resource) {
                    AbstractController.this.handleOwnsResource(resource);
                }

                @Override
                public void onUpdate(HasMetadata oldResource,
                                     HasMetadata newResource) {
                    String oldVersion = oldResource.getMetadata()
                                                   .getResourceVersion();
                    String newVersion = newResource.getMetadata()
                                                   .getResourceVersion();
                    if (Objects.equals(oldVersion, newVersion)) {
                        return;
                    }
                    AbstractController.this.handleOwnsResource(newResource);
                }

                @Override
                public void onDelete(HasMetadata resource, boolean b) {
                    // Do nothing
                }
            });

            this.informerMap.put(ownsClass, informer);
        });
    }

    @SuppressWarnings("unchecked")
    protected T getCRByKey(String key) {
        SharedIndexInformer<T> informer = (SharedIndexInformer<T>)
                                          this.informerMap.get(this.crClass);
        return informer.getIndexer().getByKey(key);
    }

    @SuppressWarnings("unchecked")
    protected <R extends HasMetadata> R getResourceByKey(String key,
                                                         Class<R> rClass) {
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        return informer.getIndexer().getByKey(key);
    }

    @SuppressWarnings("unchecked")
    protected <R extends HasMetadata> List<R> getResourceList(String namespace,
                                                              Class<R> rClass) {
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        return informer.getIndexer().byIndex(namespace, Cache.NAMESPACE_INDEX);
    }

    private void handleOwnsResource(HasMetadata resource) {
        OwnerReference owner = this.getControllerOf(resource);
        if (owner == null || StringUtils.isBlank(owner.getName()) ||
            StringUtils.equalsIgnoreCase(HugeGraphComputerJob.KIND,
                                         owner.getKind())) {
            return;
        }
        String namespace = resource.getMetadata().getNamespace();
        Request request = new Request(namespace, owner.getName());
        this.enqueueRequest(request);
    }

    private OwnerReference getControllerOf(HasMetadata resource) {
        List<OwnerReference> ownerReferences = resource.getMetadata()
                                                       .getOwnerReferences();
        for (OwnerReference ownerReference : ownerReferences) {
            if (Boolean.TRUE.equals(ownerReference.getController())) {
                return ownerReference;
            }
        }
        return null;
    }

    private void enqueueRequest(Request request) {
        if (request != null) {
            LOG.info("Adding to enqueue request: {}", request);
            this.workQueue.add(request);
        }
    }

    private boolean hasSynced() {
        boolean synced = true;
        if (MapUtils.isEmpty(this.informerMap)) {
            return synced;
        }
        synced = KubeUtil
                .waitUntilReady(Duration.ZERO,
                                this.readyCheckInternal,
                                this.readyTimeout,
                                () -> {
                                return this.informerMap
                                           .values().stream()
                                           .allMatch(SharedInformer::hasSynced);
                                },
                                this.executor);
        return synced;
    }

    @SuppressWarnings("unchecked")
    private Class<T> crClass() {
        Type type = this.getClass().getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType)type;
        Type[] types = parameterizedType.getActualTypeArguments();
        return (Class<T>) types[0];
    }
}
