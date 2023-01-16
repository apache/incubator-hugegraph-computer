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

package org.apache.hugegraph.computer.core.bsp;

import java.util.List;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.ContainerInfo;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.SuperstepStat;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.util.SerializeUtil;
import org.apache.hugegraph.computer.core.worker.WorkerStat;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class Bsp4Worker extends BspBase {

    private static final Logger LOG = Log.logger(Bsp4Worker.class);

    private final ContainerInfo workerInfo;

    public Bsp4Worker(Config config, ContainerInfo workerInfo) {
        super(config);
        this.workerInfo = workerInfo;
    }

    /**
     * Wait master registered, get master's information includes hostname
     * and port.
     */
    public ContainerInfo waitMasterInitDone() {
        LOG.info("Worker({}) is waiting for master init-done",
                 this.workerInfo.uniqueName());
        String path = this.constructPath(BspEvent.BSP_MASTER_INIT_DONE);
        byte[] bytes = this.bspClient().get(path, this.registerTimeout(),
                                            this.logInterval());
        ContainerInfo masterInfo = new ContainerInfo();
        SerializeUtil.fromBytes(bytes, masterInfo);
        LOG.info("Worker({}) waited master init-done: {}",
                 this.workerInfo.uniqueName(), masterInfo);
        return masterInfo;
    }

    /**
     * Register this worker, worker's information is passed by constructor.
     */
    public void workerInitDone() {
        /*
         * Can't use workerInfo.id(), because the master does not assign
         * worker id yet. The master assigns worker's id by signal
         * BspEvent.BSP_MASTER_ALL_INIT_DONE. Worker get it through method
         * {@link #waitMasterAllInitDone()}.
         */
        String path = this.constructPath(BspEvent.BSP_WORKER_INIT_DONE,
                                         this.workerInfo.uniqueName());
        this.bspClient().put(path, SerializeUtil.toBytes(this.workerInfo));
        LOG.info("Worker set init-done: {}", this.workerInfo.uniqueName());
    }

    /**
     * Get all workers information includes hostname and port the workers
     * listen on.
     */
    public List<ContainerInfo> waitMasterAllInitDone() {
        LOG.info("Worker({}) is waiting for master all-init-done",
                 this.workerInfo.id());
        String path = this.constructPath(BspEvent.BSP_MASTER_ALL_INIT_DONE);
        byte[] serializedContainers = this.bspClient().get(
                                      path,
                                      this.registerTimeout(),
                                      this.logInterval());
        List<ContainerInfo> containers = SerializeUtil.fromBytes(
                                         serializedContainers,
                                         ContainerInfo::new);
        this.assignThisWorkerId(containers);
        LOG.info("Worker({}) waited master all-init-done, workers: {}",
                 this.workerInfo.id(), containers);
        return containers;
    }

    /**
     * The master set this signal to let workers knows the first superstep to
     * start with.
     */
    public int waitMasterResumeDone() {
        LOG.info("Worker({}) is waiting for master resume-done",
                 this.workerInfo.id());
        String path = this.constructPath(BspEvent.BSP_MASTER_RESUME_DONE);
        byte[] bytes = this.bspClient().get(path, this.barrierOnMasterTimeout(),
                                            this.logInterval());
        IntValue superstep = new IntValue();
        SerializeUtil.fromBytes(bytes, superstep);
        LOG.info("Worker({}) waited master resume-done({})",
                 this.workerInfo.id(), superstep.value());
        return superstep.value();
    }

    /**
     * Set read done signal after read input splits, and send all vertices and
     * edges to correspond workers.
     */
    public void workerInputDone() {
        String path = this.constructPath(BspEvent.BSP_WORKER_INPUT_DONE,
                                         this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set input-done", this.workerInfo.id());
    }

    /**
     * Wait master signal that all workers input done. After this, worker
     * can merge the vertices and edges.
     */
    public void waitMasterInputDone() {
        LOG.info("Worker({}) is waiting for master input-done",
                 this.workerInfo.id());
        String path = this.constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        this.bspClient().get(path, this.barrierOnMasterTimeout(),
                             this.logInterval());
        LOG.info("Worker({}) waited master input-done", this.workerInfo.id());
    }

    /**
     * Worker set this signal to indicate the worker is ready to receive
     * messages from other workers.
     */
    public void workerStepPrepareDone(int superstep) {
        String path = this.constructPath(BspEvent.BSP_WORKER_STEP_PREPARE_DONE,
                                         superstep, this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set superstep-prepare-done({})",
                 this.workerInfo.id(), superstep);
    }

    /**
     * After receive this signal, the worker can execute and send messages
     * to other workers.
     */
    public void waitMasterStepPrepareDone(int superstep) {
        LOG.info("Worker({}) is waiting for master superstep-prepare-done({})",
                 this.workerInfo.id(), superstep);
        String path = this.constructPath(BspEvent.BSP_MASTER_STEP_PREPARE_DONE,
                                         superstep);
        this.bspClient().get(path, this.barrierOnMasterTimeout(),
                             this.logInterval());
        LOG.info("Worker({}) waited master superstep-prepare-done({})",
                 this.workerInfo.id(), superstep);
    }

    /**
     * Worker set this signal to indicate the worker has computed the
     * vertices for specified superstep.
     */
    public void workerStepComputeDone(int superstep) {
        String path = this.constructPath(BspEvent.BSP_WORKER_STEP_COMPUTE_DONE,
                                         superstep, this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set superstep-compute-done({})",
                 this.workerInfo.id(), superstep);
    }

    /**
     * After receive this signal, it indicates that all workers have computed
     * vertices for the superstep. The worker can calls after-superstep callback
     * of managers.
     */
    public void waitMasterStepComputeDone(int superstep) {
        LOG.info("Worker({}) is waiting for master superstep-compute-done({})",
                 this.workerInfo.id(), superstep);
        String path = this.constructPath(BspEvent.BSP_MASTER_STEP_COMPUTE_DONE,
                                         superstep);
        this.bspClient().get(path, this.barrierOnMasterTimeout(),
                             this.logInterval());
        LOG.info("Worker({}) waited master superstep-compute-done({})",
                 this.workerInfo.id(), superstep);
    }

    /**
     * Worker set this signal after sent all messages to corresponding
     * workers and sent aggregators to master.
     */
    public void workerStepDone(int superstep, WorkerStat workerStat) {
        String path = this.constructPath(BspEvent.BSP_WORKER_STEP_DONE,
                                         superstep, this.workerInfo.id());
        this.bspClient().put(path, SerializeUtil.toBytes(workerStat));
        LOG.info("Worker({}) set superstep-done({}), worker stat: {}",
                 this.workerInfo.id(), superstep, workerStat);
    }

    /**
     * The master set this signal after all workers signaled superstepDone,
     * and master computes MasterComputation, and broadcast all aggregators to
     * works.
     */
    public SuperstepStat waitMasterStepDone(int superstep) {
        LOG.info("Worker({}) is waiting for master superstep-done({})",
                 this.workerInfo.id(), superstep);
        String path = this.constructPath(BspEvent.BSP_MASTER_STEP_DONE,
                                         superstep);
        byte[] bytes = this.bspClient().get(path, this.barrierOnMasterTimeout(),
                                            this.logInterval());
        SuperstepStat superstepStat = new SuperstepStat();
        SerializeUtil.fromBytes(bytes, superstepStat);
        LOG.info("Worker({}) waited master superstep-done({}), graph stat: {}",
                 this.workerInfo.id(), superstep, superstepStat);
        return superstepStat;
    }

    /**
     * Worker set this signal to indicate the worker has outputted the result.
     */
    public void workerOutputDone() {
        String path = this.constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE,
                                         this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set output-done", this.workerInfo.id());
    }

    /**
     * Worker set this signal to indicate the worker has stopped the managers
     * and will successfully exit.
     */
    public void workerCloseDone() {
        String path = this.constructPath(BspEvent.BSP_WORKER_CLOSE_DONE,
                                         this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set close-done", this.workerInfo.id());
    }

    // Note: The workerInfo in Bsp4Worker is the same object in WorkerService.
    private void assignThisWorkerId(List<ContainerInfo> workersFromMaster) {
        for (ContainerInfo container : workersFromMaster) {
            if (this.workerInfo.uniqueName().equals(container.uniqueName())) {
                this.workerInfo.id(container.id());
                LOG.info("Worker({}) assigned id {} from master",
                         this.workerInfo.uniqueName(), this.workerInfo.id());
                break;
            }
        }
    }
}
