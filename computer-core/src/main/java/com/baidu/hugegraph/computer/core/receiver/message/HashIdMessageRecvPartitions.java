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

package com.baidu.hugegraph.computer.core.receiver.message;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvPartitions;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.SuperstepFileGenerator;

public class HashIdMessageRecvPartitions
       extends MessageRecvPartitions<HashIdMessageRecvPartition> {

    public HashIdMessageRecvPartitions(ComputerContext context,
                                        SuperstepFileGenerator fileGenerator,
                                        SortManager sortManager) {
        super(context, fileGenerator, sortManager);
    }

    @Override
    public HashIdMessageRecvPartition createPartition() {
        return new HashIdMessageRecvPartition(this.context, this.fileGenerator,
                                               this.sortManager);
    }
}
