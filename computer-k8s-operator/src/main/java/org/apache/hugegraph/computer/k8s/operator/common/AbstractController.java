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

package org.apache.hugegraph.computer.k8s.operator.common;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
import org.apache.hugegraph.computer.k8s.crd.model.EventType;
import org.apache.hugegraph.computer.k8s.operator.config.OperatorOptions;
import org.apache.hugegraph.computer.k8s.util.KubeUtil;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.Sets;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.fabric8.kubernetes.client.utils.Serialization;

public abstract class AbstractController<T extends CustomResource<?, ?>> {

    private static final Logger LOG = Log.logger(AbstractController.class);

    private final Class<T> crClass;
    private final String kind;
    protected final HugeConfig config;
    protected final NamespacedKubernetesClient kubeClient;
    private final WorkQueue<OperatorRequest> workQueue;
    private final ScheduledExecutorService executor;
    private Set<Class<? extends HasMetadata>> ownsClassSet;
    private Map<Class<? extends HasMetadata>,
            SharedIndexInformer<? extends HasMetadata>> informerMap;

    public AbstractController(HugeConfig config,
                              NamespacedKubernetesClient kubeClient) {
        this.config = config;
        this.crClass = this.getCRClass();
        this.kind = HasMetadata.getKind(this.crClass).toLowerCase(Locale.ROOT);
        this.kubeClient = kubeClient;
        this.workQueue = new WorkQueue<>();
        Integer reconcilers = this.config.get(OperatorOptions.RECONCILER_COUNT);
        this.executor = ExecutorUtil.newScheduledThreadPool(reconcilers,
                                                            this.kind +
                                                            "-reconciler-%d");
    }

    @SafeVarargs
    public final void register(SharedInformerFactory informerFactory,
                               Class<? extends HasMetadata>... ownsClass) {
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

        Integer reconcilers = this.config.get(OperatorOptions.RECONCILER_COUNT);
        CountDownLatch latch = new CountDownLatch(reconcilers);
        for (int i = 0; i < reconcilers; i++) {
            this.executor.scheduleWithFixedDelay(() -> {
                        try {
                            this.startWorker();
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
        this.workQueue.shutdown();
        this.executor.shutdown();
        Long timeout = this.config
                           .get(OperatorOptions.CLOSE_RECONCILER_TIMEOUT);
        try {
            this.executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected abstract OperatorResult reconcile(OperatorRequest request);

    protected abstract void handleFailOverLimit(OperatorRequest request,
                                                Exception e);

    private void startWorker() {
        while (!this.executor.isShutdown() &&
               !this.workQueue.isShutdown() &&
               !Thread.currentThread().isInterrupted()) {
            LOG.debug("Trying to get item from work queue of {}-controller",
                      this.kind);
            OperatorRequest request = null;
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
            OperatorResult result;
            try {
                LOG.debug("Start reconcile request: {}", request);
                result = this.reconcile(request);
            } catch (Exception e) {
                LOG.error("Reconcile occur error, requeue request: ", e);
                int maxReconcileRetry = this.config.get(
                                        OperatorOptions.MAX_RECONCILE_RETRY);
                if (request.retryIncrGet() > maxReconcileRetry) {
                    try {
                        this.handleFailOverLimit(request, e);
                    } catch (Exception e2) {
                        LOG.error("Reconcile fail over limit occur error:", e2);
                    }
                    result = OperatorResult.NO_REQUEUE;
                } else {
                    result = OperatorResult.REQUEUE;
                }
            }

            try {
                if (result.requeue()) {
                    this.enqueueRequest(request);
                }
            } finally {
                LOG.debug("End reconcile request: {}", request);
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
                LOG.info("Received a CR add request: {}", cr.getMetadata());
                OperatorRequest request = OperatorRequest.parseRequestByCR(cr);
                AbstractController.this.enqueueRequest(request);
            }

            @Override
            public void onUpdate(T oldCR, T cr) {
                LOG.info("Received a CR update request: {}", cr.getMetadata());
                OperatorRequest request = OperatorRequest.parseRequestByCR(cr);
                AbstractController.this.enqueueRequest(request);
            }

            @Override
            public void onDelete(T cr, boolean deletedStateUnknown) {
                LOG.info("Received a CR delete request: {}", cr.getMetadata());
                OperatorRequest request = OperatorRequest.parseRequestByCR(cr);
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

    protected T getCR(OperatorRequest request) {
        return this.getResourceByName(request.namespace(), request.name(),
                                      this.crClass);
    }

    protected <R extends HasMetadata> R getResourceByName(String namespace,
                                                          String name,
                                                          Class<R> rClass) {
        @SuppressWarnings("unchecked")
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        String key = new OperatorRequest(namespace, name).key();
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
                             .byIndex(Cache.NAMESPACE_INDEX, namespace);
        if (CollectionUtils.isEmpty(rs)) {
            return rs;
        }
        for (int i = 0; i < rs.size(); i++) {
            rs.set(i, Serialization.clone(rs.get(i)));
        }
        return rs;
    }

    protected <R extends HasMetadata> List<R> getResourceListWithLabels(
                                              String namespace, Class<R> rClass,
                                              Map<String, String> matchLabels) {
        @SuppressWarnings("unchecked")
        SharedIndexInformer<R> informer = (SharedIndexInformer<R>)
                                          this.informerMap.get(rClass);
        List<R> rs = informer.getIndexer()
                             .byIndex(Cache.NAMESPACE_INDEX, namespace);
        if (CollectionUtils.isEmpty(rs)) {
            return rs;
        }

        if (MapUtils.isEmpty(matchLabels)) {
            return this.getResourceList(namespace, rClass);
        }

        List<R> matchRS = new ArrayList<>();

        Set<Map.Entry<String, String>> matchLabelSet = matchLabels.entrySet();
        for (R r : rs) {
            Map<String, String> label = KubernetesResourceUtil
                                        .getLabels(r.getMetadata());
            if (MapUtils.isEmpty(label)) {
                continue;
            }
            if (label.entrySet().containsAll(matchLabelSet)) {
                matchRS.add(Serialization.clone(r));
            }
        }

        return matchRS;
    }

    protected List<Pod> getPodsByJob(Job job) {
        String namespace = job.getMetadata().getNamespace();
        LabelSelector selector = job.getSpec().getSelector();

        if (selector != null) {
            Map<String, String> matchLabels = selector.getMatchLabels();
            if (MapUtils.isNotEmpty(matchLabels)) {
                return this.getResourceListWithLabels(namespace, Pod.class,
                                                      matchLabels);
            }
        }
        return Collections.emptyList();
    }

    protected void recordEvent(HasMetadata eventRef,
                               EventType eventType, String eventName,
                               String reason, String message) {
        String component = HasMetadata.getKind(this.crClass) + "Operator";
        EventSource eventSource = new EventSource();
        eventSource.setComponent(component);
        Event event = KubeUtil.buildEvent(eventRef, eventSource,
                                          eventType, eventName,
                                          reason, message);
        KubernetesClient client = this.kubeClient;
        String namespace = event.getMetadata().getNamespace();
        if (!Objects.equals(this.kubeClient.getNamespace(), namespace)) {
            client = this.kubeClient.inNamespace(namespace);
        }
        client.v1().events().createOrReplace(event);
    }

    private void handleOwnsResource(HasMetadata ownsResource) {
        MatchWithMsg ownsMatch = this.ownsPredicate(ownsResource);
        if (ownsMatch.isMatch()) {
            String namespace = ownsResource.getMetadata().getNamespace();
            OperatorRequest request = new OperatorRequest(namespace,
                                                          ownsMatch.msg());
            this.enqueueRequest(request);
        }
    }

    protected MatchWithMsg ownsPredicate(HasMetadata ownsResource) {
        OwnerReference owner = this.getControllerOf(ownsResource);
        if (owner != null && owner.getController() &&
            StringUtils.isNotBlank(owner.getName()) &&
            StringUtils.equalsIgnoreCase(owner.getKind(), this.kind)) {
            return new MatchWithMsg(true, owner.getName());
        }
        return MatchWithMsg.NO_MATCH;
    }

    protected OwnerReference getControllerOf(HasMetadata resource) {
        List<OwnerReference> ownerReferences = resource.getMetadata()
                                                       .getOwnerReferences();
        for (OwnerReference ownerReference : ownerReferences) {
            if (Boolean.TRUE.equals(ownerReference.getController())) {
                return ownerReference;
            }
        }
        return null;
    }

    private void enqueueRequest(OperatorRequest request) {
        if (request != null) {
            LOG.debug("Enqueue a resource request: {}", request);
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

    private Class<T> getCRClass() {
        Type type = this.getClass().getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type[] types = parameterizedType.getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<T> crClass = (Class<T>) types[0];
        return crClass;
    }
}
