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

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.driver.util.JsonUtil;
import com.baidu.hugegraph.util.E;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.informers.cache.Cache;

public class Request {

    private String name;
    private String namespace;

    public static Request parseRequestByCR(CustomResource<?, ?> resource) {
        E.checkNotNull(resource, "resource");
        ObjectMeta metadata = resource.getMetadata();
        E.checkNotNull(resource, "metadata");
        String name = metadata.getName();
        E.checkArgument(StringUtils.isNotBlank(name), "resourceName");
        return new Request(metadata.getNamespace(), name);
    }

    public Request(String name) {
        this(null, name);
    }

    public Request(String namespace, String name) {
        this.name = name;
        this.namespace = namespace;
    }

    public String name() {
        return this.name;
    }

    public Request name(String name) {
        this.name = name;
        return this;
    }

    public String namespace() {
        return this.namespace;
    }

    public Request namespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Request)) {
            return false;
        }

        final Request other = (Request) obj;
        return Objects.equals(other.namespace, this.namespace) &&
               Objects.equals(other.name, this.name);
    }

    public String key() {
        return Cache.namespaceKeyFunc(this.namespace, this.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.namespace, this.name);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
