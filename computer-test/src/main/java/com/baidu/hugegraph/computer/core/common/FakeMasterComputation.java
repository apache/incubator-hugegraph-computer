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

package com.baidu.hugegraph.computer.core.common;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator4Master;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.master.MasterComputation;
import com.baidu.hugegraph.computer.core.master.MasterContext;

/**
 * Can't create instance, used for ObjectFactoryTest.
 */
public class FakeMasterComputation implements MasterComputation {

    private FakeMasterComputation() {
    }

    @Override
    public void init(Config config, Aggregator4Master aggregator4Master) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean compute(MasterContext context) {
        return false;
    }
}
