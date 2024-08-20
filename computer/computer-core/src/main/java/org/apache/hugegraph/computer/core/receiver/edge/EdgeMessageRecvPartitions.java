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

package org.apache.hugegraph.computer.core.receiver.edge;

import java.util.List;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageRecvPartitions;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;

public class EdgeMessageRecvPartitions
       extends MessageRecvPartitions<EdgeMessageRecvPartition> {

    public EdgeMessageRecvPartitions(ComputerContext context,
                                     SuperstepFileGenerator fileGenerator,
                                     SortManager sortManager,
                                     SnapshotManager snapshotManager) {
        super(context, fileGenerator, sortManager, snapshotManager);
    }

    @Override
    public EdgeMessageRecvPartition createPartition() {
        return new EdgeMessageRecvPartition(this.context, this.fileGenerator,
                                            this.sortManager);
    }

    @Override
    public void writePartitionSnapshot(int partitionId, List<String> outputFiles) {
        if (this.snapshotManager.writeSnapshot()) {
            this.snapshotManager.upload(MessageType.EDGE, partitionId, outputFiles);
        }
    }
}
