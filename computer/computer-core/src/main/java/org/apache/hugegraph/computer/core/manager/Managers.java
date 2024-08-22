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

package org.apache.hugegraph.computer.core.manager;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;

public class Managers implements Iterable<Manager> {

    private final Map<String, Manager> managers;

    public Managers() {
        this.managers = InsertionOrderUtil.newMap();
    }

    public <T extends Manager> T get(String name) {
        Manager manager = this.managers.get(name);
        E.checkArgumentNotNull(manager, "Not found manager '%s'", name);
        @SuppressWarnings("unchecked")
        T subManager = (T) manager;
        return subManager;
    }

    public void add(Manager manager) {
        this.managers.put(manager.name(), manager);
    }

    public Collection<Manager> all() {
        return this.managers.values();
    }

    @Override
    public Iterator<Manager> iterator() {
        return this.managers.values().iterator();
    }

    public void initAll(Config config) {
        for (Manager manager : this.all()) {
            manager.init(config);
        }
    }

    public void initedAll(Config config) {
        for (Manager manager : this.all()) {
            manager.inited(config);
        }
    }

    public void closeAll(Config config) {
        for (Manager manager : this.all()) {
            manager.close(config);
        }
    }

    public void beforeSuperstep(Config config, int superstep) {
        for (Manager manager : this.all()) {
            manager.beforeSuperstep(config, superstep);
        }
    }

    public void afterSuperstep(Config config, int superstep) {
        for (Manager manager : this.all()) {
            manager.afterSuperstep(config, superstep);
        }
    }
}
