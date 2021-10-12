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

package com.baidu.hugegraph.computer.core.bsp;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.watch.WatchEvent;
import io.grpc.stub.StreamObserver;

public class DistributedLock {

    private final Client client;
    private final Lock lockClient;
    private final Lease leaseClient;
    private static DistributedLock lockProvider;

    private DistributedLock(Client client) {
        this.lockClient = client.getLockClient();
        this.leaseClient = client.getLeaseClient();
        this.client = client;
    }

    public synchronized static DistributedLock initLock(Client client) {
        if (lockProvider == null) {
            lockProvider = new DistributedLock(client);
        }
        return lockProvider;
    }

    public static DistributedLock getInstance() {
        if (lockProvider == null) {
            throw new ComputerException("DistributedLock uninitialized");
        } else {
            return lockProvider;
        }
    }

    public synchronized LockResult tryLock(String key, long ttl, TimeUnit unit)
                                   throws ExecutionException,
                                          InterruptedException {
        long leaseId = this.leaseClient.grant(unit.toSeconds(ttl))
                                       .get().getID();
        this.leaseClient.keepAlive(leaseId,
                                   new StreamObserver<LeaseKeepAliveResponse>() {
               @Override
               public void onNext(
                           LeaseKeepAliveResponse leaseKeepAliveResponse) {

               }

               @Override
               public void onError(Throwable throwable) {

               }

               @Override
               public void onCompleted() {

               }
        });
        ByteSequence lockKey = ByteSequence.from(
                               key.getBytes(StandardCharsets.UTF_8));
        LockResult result = new LockResult();
        try {
            LockResponse lockResponse = this.lockClient
                                            .lock(lockKey, leaseId).get();
            result.leaseId(leaseId);
            result.revision(lockResponse.getHeader().getRevision());
            result.lockSuccess();
        } catch (InterruptedException | ExecutionException e) {
            result.lockFailed();
        }
        return result;
    }

    public synchronized LockResult lock(String key)
                                   throws ExecutionException,
                                          InterruptedException {
        LockResult result = this.tryLock(key, 30, TimeUnit.SECONDS);
        if (result != null && result.isLockSuccess) {
            return result;
        }
        ByteSequence lockKey = ByteSequence.from(
                               key.getBytes(StandardCharsets.UTF_8));

        CompletableFuture<Void> future = new CompletableFuture<>();
        Watch.Watcher watch =
                this.client.getWatchClient().watch(lockKey, watchResponse -> {
                    for (WatchEvent event : watchResponse.getEvents()) {
                        if (event.getEventType()
                                 .equals(WatchEvent.EventType.DELETE)) {
                            future.complete(null);
                        }
                    }
                });
        future.get();
        watch.close();
        return this.lock(key);
    }

    public synchronized void unlock(String key, long leaseId) {
        ByteSequence lockKey = ByteSequence.from(
                key.getBytes(StandardCharsets.UTF_8));
        this.lockClient.unlock(lockKey);
        if (leaseId != 0) {
            this.leaseClient.revoke(leaseId);
        }
    }

    public synchronized void keepAliveOnce(long leaseId) {
        this.leaseClient.keepAliveOnce(leaseId);
    }

    public static class LockResult {
        private boolean isLockSuccess;
        private long leaseId;
        private long revision;

        public LockResult() {
        }

        public void lockSuccess() {
            this.isLockSuccess = true;
        }

        public void lockFailed() {
            this.isLockSuccess = false;
        }

        public void leaseId(long leaseId) {
            this.leaseId = leaseId;
        }

        public boolean isLockSuccess() {
            return this.isLockSuccess;
        }

        public long leaseId() {
            return this.leaseId;
        }

        public void revision(long revision) {
            this.revision = revision;
        }

        public long revision() {
            return this.revision;
        }
    }
}
