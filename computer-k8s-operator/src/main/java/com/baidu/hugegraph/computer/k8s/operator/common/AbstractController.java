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

package com.baidu.hugegraph.computer.k8s.operator.common;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.operator.config.OperatorOptions;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.fabric8.kubernetes.client.utils.Serialization;

public abstract class AbstractController<T extends CustomResource<?, ?>> {

    private static final Logger LOG = Log.logger(AbstractController.class);

    protected final HugeConfig config;
    protected final String kind;
    protected final KubernetesClient kubeClient;
    private final Class<T> crClass;
    private final WorkQueue<Request> workQueue;
    private final ScheduledExecutorService executor;
    private Set<Class<? extends HasMetadata>> ownsClassSet;
    private Map<Class<? extends HasMetadata>,
            SharedIndexInformer<? extends HasMetadata>> informerMap;

    public AbstractController(HugeConfig config, KubernetesClient kubeClient) {
        this.config = config;
        this.crClass = this.crClass();
        this.kind = HasMetadata.getSingular(this.crClass);
        this.kubeClient = kubeClient;
        this.workQueue = new WorkQueue<>();
        Integer workCount = this.config.get(OperatorOptions.WORKER_COUNT);
        this.executor = ExecutorUtil.newScheduledThreadPool(workCount,
                                                            this.kind +
                                                            "-reconciler-%d");
    }

    @SafeVarargs
    public final void register(SharedInformerFactory informerFactory,
                               Class<? extends HasMetadata>...ownsClass) {
        Long resyncPeriod = this.config.get(OperatorOptions.RESYNC_PERIOD);
        this.ownsClassSet = Sets.newHashSet(ownsClass);
        this.informerMap = new ConcurrentHashMap<>();
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

        Integer workCount = this.config.get(OperatorOptions.WORKER_COUNT);
        CountDownLatch latch = new CountDownLatch(workCount);
        for (int i = 0; i < workCount; i++) {
            this.executor.scheduleWithFixedDelay(() -> {
                        try {
                            this.worker();
                        } catch (Throwable e) {
                            LOG.error("Unexpected reconcile loop abortion", e);
                        } finally {
                            latch.countDown();
                        }
                    },
                    Duration.ZERO.toMillis(),
                    Duration.ofSeconds(1L).toMillis(), TimeUnit.MILLISECONDS
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
        this.workQueue.shutDown();
        this.executor.shutdown();
    }

    protected abstract Result reconcile(Request request);

    private void worker() {
        while (!this.executor.isShutdown() &&
               !this.workQueue.isShutdown() &&
               !Thread.currentThread().isInterrupted()) {
            LOG.debug("Trying to get item from work queue of {}-controller",
                      this.kind);
            Request request = null;
            try {
                request = this.workQueue.get();
            } catch (InterruptedException e) {
                LOG.warn("The {}-controller worker interrupted...",
                         this.kind, e);
            }
            if (request == null) {
                LOG.info("The {}-controller worker exiting...", this.kind);
                break;
            }
            Result result;
            try {
                result = this.reconcile(request);
            } catch (Exception e) {
                LOG.error("Reconcile occur error, requeue request: ", e);
                result = Result.REQUEUE;
            }

            try {
                if (result.requeue()) {
                    this.enqueueRequest(request);
                }
            } finally {
                this.workQueue.done(request);
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
                    AbstractController.this.handleOwnsResource(resource);
                }
            });

            this.informerMap.put(ownsClass, informer);
        });
    }

    protected T getCR(Request request) {
        return this.getResourceByName(request.namespace(), request.name(),
                                      this.crClass);
    }

    protected <R extends HasMetadata> R getResourceByName(String namespace,
                                                          String name,
                                                          Class<R> rClass) {
        @SuppressWarnings("unchecked")
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        String key = new Request(namespace, name).key();
        R resource = informer.getIndexer().getByKey(key);
        if (resource == null) {
            return null;
        }
        return Serialization.clone(resource);
    }

    protected <R extends HasMetadata> List<R> getResourceList(String namespace,
                                                              Class<R> rClass) {
        @SuppressWarnings("unchecked")
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        List<R> rs = informer.getIndexer()
                             .byIndex(namespace, Cache.NAMESPACE_INDEX);
        if (CollectionUtils.isEmpty(rs)) {
            return rs;
        }
        return Serialization.clone(rs);
    }

    protected <R extends HasMetadata> List<R> getResourceListWithLabels(
                                              String namespace, Class<R> rClass,
                                              Map<String, String> matchLabels) {
        @SuppressWarnings("unchecked")
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        List<R> rs = informer.getIndexer()
                             .byIndex(namespace, Cache.NAMESPACE_INDEX);
        if (CollectionUtils.isEmpty(rs)) {
            return rs;
        }

        if (MapUtils.isEmpty(matchLabels)) {
            return this.getResourceList(namespace, rClass);
        }

        List<R> matchRS = Lists.newArrayList();

        Set<Map.Entry<String, String>> matchLabelSet = matchLabels.entrySet();
        for (R r : rs) {
            Map<String, String> label = KubernetesResourceUtil
                                        .getLabels(r.getMetadata());
            if (MapUtils.isEmpty(label)) {
                continue;
            }
            if (label.entrySet().containsAll(matchLabelSet)) {
                matchRS.add(r);
            }
        }

        return Serialization.clone(matchRS);
    }

    protected List<Pod> getPodsByLabels(Job job) {
        Map<String, String> matchLabels = job.getSpec().getSelector()
                                             .getMatchLabels();

        String labelString = KubeUtil.map2LabelString(matchLabels);
        ListOptions listOptions = new ListOptionsBuilder()
                .withLabelSelector(labelString)
                .build();
        PodList list = this.kubeClient
                .pods()
                .inNamespace(job.getMetadata().getNamespace())
                .list(listOptions);
        return list.getItems();
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
            LOG.info("Enqueue an request: {}", request);
            this.workQueue.add(request);
        }
    }

    private boolean hasSynced() {
        if (MapUtils.isEmpty(this.informerMap)) {
            return true;
        }

        Long internal = this.config.get(OperatorOptions.READY_CHECK_INTERNAL);
        Long timeout = this.config.get(OperatorOptions.READY_TIMEOUT);
        boolean synced;
        synced = KubeUtil
                .waitUntilReady(Duration.ZERO,
                                Duration.ofMillis(internal),
                                Duration.ofMillis(timeout),
                                () -> {
                                return this.informerMap
                                           .values().stream()
                                           .allMatch(SharedInformer::hasSynced);
                                },
                                this.executor);
        return synced;
    }

    private Class<T> crClass() {
        Type type = this.getClass().getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type[] types = parameterizedType.getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<T> crClass = (Class<T>) types[0];
        return crClass;
    }
}
