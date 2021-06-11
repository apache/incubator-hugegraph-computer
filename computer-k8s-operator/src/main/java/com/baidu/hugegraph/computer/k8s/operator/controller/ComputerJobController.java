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

import java.time.Instant;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatusBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.operator.common.AbstractController;
import com.baidu.hugegraph.computer.k8s.operator.common.Request;
import com.baidu.hugegraph.computer.k8s.operator.common.Result;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class ComputerJobController
       extends AbstractController<HugeGraphComputerJob> {

    private static final Logger LOG = Log.logger(AbstractController.class);

    private final MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
            Resource<HugeGraphComputerJob>> operation;


    public ComputerJobController(KubernetesClient kubeClient) {
        super(kubeClient);
        this.operation = this.kubeClient.customResources(
                                         HugeGraphComputerJob.class,
                                         HugeGraphComputerJobList.class);
    }

    @Override
    protected Result reconcile(Request request) {
        HugeGraphComputerJob computerJob = this.getCRByKey(request.key());
        if (computerJob == null) {
            LOG.info("Unable to fetch HugeGraphComputerJob, " +
                     "it may have been deleted");
            return Result.NO_REQUEUE;
        }

        if (computerJob.getStatus() == null) {
            // create
            ComputerJobStatus status = new ComputerJobStatusBuilder()
                    .withJobStatus(JobStatus.INITIALIZING.name())
                    .withLastUpdateTime(Instant.now().toString())
                    .build();
            computerJob.setStatus(status);
        }

        this.operation.inNamespace(request.namespace())
                      .updateStatus(computerJob);
        // TODO: implement it
        return Result.NO_REQUEUE;
    }

    @Override
    protected void onCRDelete(HugeGraphComputerJob cr,
                              boolean deletedStateUnknown) {

    }
}
