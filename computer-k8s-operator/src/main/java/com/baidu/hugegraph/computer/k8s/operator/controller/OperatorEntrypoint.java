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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

public class OperatorEntrypoint {

    private static final Logger LOG = Log.logger(OperatorEntrypoint.class);
    private static final int RESYNC_PERIOD = 10 * 60 * 1000;

    public static void main(String[] args) {
        SharedInformerFactory informerFactory = null;
        ComputerJobController jobController = null;
        try (KubernetesClient kubeClient = new DefaultKubernetesClient()) {
            String namespace = kubeClient.getNamespace();
            if (namespace == null) {
                LOG.warn("No namespace found via config, assuming default.");
                namespace = "default";
            }
            LOG.info("Using namespace : " + namespace);

            informerFactory = kubeClient.informers();

            jobController = new ComputerJobController(HugeGraphComputerJob.KIND,
                                                      kubeClient);
            jobController.register(informerFactory, RESYNC_PERIOD,
                                   Deployment.class, ConfigMap.class);

            informerFactory.startAllRegisteredInformers();
            informerFactory.addSharedInformerEventListener(exception -> {
                LOG.error("Exception occurred, but caught", exception);
            });
            jobController.run();
        } catch (Throwable throwable) {
            if (jobController != null) {
                jobController.shutdown();
            }
            if (informerFactory != null) {
                informerFactory.stopAllRegisteredInformers();
            }
            LOG.error("Kubernetes Client Exception: ", throwable);
        }
    }
}
