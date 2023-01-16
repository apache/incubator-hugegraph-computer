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

package org.apache.hugegraph.computer.driver;

import org.apache.hugegraph.computer.driver.util.JsonUtil;

/**
 * It is sent from master to workers.
 */
public class SuperstepStat {

    private long vertexCount;
    private long edgeCount;
    private long finishedVertexCount;
    private long messageCount;
    private long messageBytes;
    private boolean active;

    public SuperstepStat() {
        this.active = true;
    }

    public void vertexCount(long vertexCount) {
        this.vertexCount = vertexCount;
    }

    public void edgeCount(long edgeCount) {
        this.edgeCount = edgeCount;
    }

    public void finishedVertexCount(long finishedVertexCount) {
        this.finishedVertexCount = finishedVertexCount;
    }

    public void messageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public void messageBytes(long messageBytes) {
        this.messageBytes = messageBytes;
    }

    public void active(boolean active) {
        this.active = active;
    }

    public long vertexCount() {
        return this.vertexCount;
    }

    public long edgeCount() {
        return this.edgeCount;
    }

    public long finishedVertexCount() {
        return this.finishedVertexCount;
    }

    public long messageCount() {
        return this.messageCount;
    }

    public long messageBytes() {
        return this.messageBytes;
    }

    public void inactivate() {
        this.active = false;
    }

    public boolean active() {
        return this.active;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SuperstepStat)) {
            return false;
        }
        SuperstepStat other = (SuperstepStat) obj;
        return this.vertexCount == other.vertexCount &&
               this.edgeCount == other.edgeCount &&
               this.finishedVertexCount == other.finishedVertexCount &&
               this.messageCount == other.messageCount &&
               this.messageBytes == other.messageBytes &&
               this.active == other.active;
    }

    @Override
    public int hashCode() {
        return (Long.hashCode(this.vertexCount) >>> 56) ^
               (Long.hashCode(this.edgeCount) >>> 48) ^
               (Long.hashCode(this.messageCount) >>> 40) ^
               (Long.hashCode(this.edgeCount) >>> 32) ^
               (Long.hashCode(this.finishedVertexCount) >>> 24) ^
               (Long.hashCode(this.messageCount) >>> 16) ^
               (Long.hashCode(this.messageBytes) >>> 8) ^
               Boolean.hashCode(this.active);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
