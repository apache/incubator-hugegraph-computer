/*
 *  Copyright 2017 HugeGraph Authors
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with this
 *  work for additional information regarding copyright ownership. The ASF
 *  licenses this file to You under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.baidu.hugegraph.computer.core.bsp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.annotations.VisibleForTesting;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.GetOption.SortOrder;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchEvent.EventType;
import io.etcd.jetcd.watch.WatchResponse;


public class EtcdClient {

    private static final Logger LOG = Log.logger(EtcdClient.class);
    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private final Client client;
    private final Watch watch;
    private final KV kv;

    public EtcdClient(String endpoints, String namespace) {
        E.checkArgumentNotNull(endpoints,
                               "The endpoints can't be null");
        E.checkArgumentNotNull(namespace,
                               "The namespace can't be null");
        ByteSequence namespaceSeq = ByteSequence.from(namespace.getBytes(
                                                                ENCODING));
        this.client = Client.builder().endpoints(endpoints)
                            .namespace(namespaceSeq).build();
        this.watch = this.client.getWatchClient();
        this.kv = this.client.getKVClient();
    }

    /**
     * Put the key value mapping to the map, if the map previously contained a
     * mapping for the key, the old value is replaced by the specified value.
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */
    public void put(String key, byte[] value) {
        E.checkArgument(key != null,
                        "The key can't be null.");
        E.checkArgument(value != null,
                        "The value can't be null.");
        try {
            this.kv.put(ByteSequence.from(key, ENCODING),
                        ByteSequence.from(value))
                   .get();
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while putting with key='%s'", e, key);
        } catch (ExecutionException e) {
            throw new ComputerException("Error while putting with key='%s'",
                                        e, key);
        }
    }

    /**
     * Returns the value to which the specified key is mapped.
     * @param key The key to be found
     * @return the value of specified key, null if not found
     */
    public byte[] get(String key) {
        return this.get(key, false);
    }

    /**
     * Returns the value to which the specified key is mapped.
     * @param key The key to be found
     * @param throwException whether to throw ComputerException if not found.
     * @return the value of specified key, null if not found and
     * throwException is set false
     * @throws ComputerException if not found and throwException is set true
     */
    public byte[] get(String key, boolean throwException) {
        E.checkArgumentNotNull(key, "The key can't be null");
        try {
            ByteSequence keySeq = ByteSequence.from(key, ENCODING);
            GetResponse response = this.kv.get(keySeq).get();
            if (response.getCount() > 0) {
                List<KeyValue> kvs = response.getKvs();
                assert kvs.size() == 1;
                return kvs.get(0).getValue().getBytes();
            } else if (throwException) {
                 throw new ComputerException("Can't find value for key='%s'",
                                             key);
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while getting with key='%s'", e, key);
        } catch (ExecutionException e) {
            throw new ComputerException("Error while getting with key='%s'",
                                        e, key);
        }
    }

    /**
     * Returns the value to which the specified key is mapped. If no
     * key exists, wait at most timeout milliseconds. Or throw
     * ComputerException if timeout
     * @param key the key whose associated value is to be returned.
     * @param timeout the max time in milliseconds to wait.
     * @return the specified value in byte array to which the specified key is
     * mapped.
     */
    public byte[] get(String key, long timeout) {
        E.checkArgumentNotNull(key, "The key can't be null");
        E.checkArgument(timeout > 0L,
                        "The timeout must be > 0, but got: %s", timeout);
        long deadline = System.currentTimeMillis() + timeout;
        ByteSequence keySeq = ByteSequence.from(key, ENCODING);
        try {
            GetResponse response = this.kv.get(keySeq).get();
            if (response.getCount() > 0) {
                List<KeyValue> kvs = response.getKvs();
                return kvs.get(0).getValue().getBytes();
            } else {
                timeout = deadline - System.currentTimeMillis();
                if (timeout > 0) {
                    long revision = response.getHeader().getRevision();
                    return this.waitAndGetFromPutEvent(keySeq, revision,
                                                       timeout);
                } else {
                    throw new ComputerException("Can't find value for key='%s'",
                                                key);
                }
            }
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while getting with key='%s'",
                      e, key);
        } catch (ExecutionException e) {
            throw new ComputerException("Error while getting with key='%s'",
                                        e, key);
        }
    }

    /**
     * Wait put event.
     * Return the value from event if event triggered in timeout.
     * @throws ComputerException if no event triggered in timeout
     */
    private byte[] waitAndGetFromPutEvent(ByteSequence keySeq, long revision,
                                          long timeout)
                                          throws InterruptedException {
        AtomicReference<byte[]> eventValue = new AtomicReference<>();
        final BarrierEvent barrierEvent = new BarrierEvent();
        WatchOption watchOption = WatchOption.newBuilder()
                                             .withRevision(revision)
                                             .withNoDelete(true)
                                             .build();
        Consumer<WatchResponse> consumer = watchResponse -> {
            List<WatchEvent> events = watchResponse.getEvents();
            for (WatchEvent event : events) {
                if (EventType.PUT.equals(event.getEventType())) {
                    KeyValue keyValue = event.getKeyValue();
                    if (keySeq.equals(keyValue.getKey())) {
                        eventValue.set(event.getKeyValue().getValue()
                                            .getBytes());
                        barrierEvent.signalAll();
                        return;
                    } else {
                        assert false;
                        throw new ComputerException(
                                  "Expect event key '%s', found '%s'",
                                  keySeq.toString(ENCODING),
                                  keyValue.getKey().toString(ENCODING));
                    }
                } else {
                    assert false;
                    throw new ComputerException("Unexpected event type '%s'",
                                                event.getEventType());
                }
            }
        };
        Watch.Watcher watcher = this.watch.watch(keySeq, watchOption, consumer);
        barrierEvent.await(timeout);
        watcher.close();
        byte[] value = eventValue.get();
        if (value != null) {
            return value;
        } else {
            throw new ComputerException("Can't find value for key='%s'",
                                        keySeq.toString(ENCODING));
        }
    }

    /**
     * Get the values of keys with the specified prefix.
     * If no key found, return empty list.
     */
    public List<byte[]> getWithPrefix(String prefix) {
        E.checkArgumentNotNull(prefix, "The prefix can't be null");
        try {
            ByteSequence prefixSeq = ByteSequence.from(prefix, ENCODING);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixSeq)
                                           .withSortOrder(SortOrder.ASCEND)
                                           .build();
            GetResponse response = this.kv.get(prefixSeq, getOption).get();
            if (response.getCount() > 0) {
                List<KeyValue> kvs = response.getKvs();
                List<byte[]> result = new ArrayList<>(kvs.size());
                for (KeyValue kv : kvs) {
                    result.add(kv.getValue().getBytes());
                }
                return result;
            } else {
                return Collections.emptyList();
            }
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while getting with prefix='%s'", e, prefix);
        } catch (ExecutionException e) {
            throw new ComputerException(
                      "Error while getting with prefix='%s'", e, prefix);
        }
    }

    /**
     * Get the expected count of values of keys with the specified prefix.
     * Throws ComputerException if there are no enough object.
     */
    public List<byte[]> getWithPrefix(String prefix, int count) {
        E.checkArgumentNotNull(prefix,
                               "The prefix can't be null");
        E.checkArgument(count >= 0,
                        "The count must be >= 0, but got: %s", count);
        try {
            ByteSequence prefixSeq = ByteSequence.from(prefix, ENCODING);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixSeq)
                                           .withLimit(count)
                                           .withSortOrder(SortOrder.ASCEND)
                                           .build();
            GetResponse response = this.kv.get(prefixSeq, getOption).get();
            if (response.getCount() == count) {
                List<KeyValue> kvs = response.getKvs();
                List<byte[]> result = new ArrayList<>(kvs.size());
                for (KeyValue kv : kvs) {
                    result.add(kv.getValue().getBytes());
                }
                return result;
            } else {
                throw new ComputerException(
                          "Expect %s elements, only find %s elements with " +
                          "prefix='%s'", count, response.getCount(), prefix);
            }
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while getting with prefix='%s', count=%s",
                      e, prefix, count);
        } catch (ExecutionException e) {
            throw new ComputerException(
                      "Error while getting with prefix='%s', count=%s", e,
                      prefix, count);
        }
    }

    /**
     * Get expected count of values with the key prefix with prefix. If there
     * is no count of keys, wait at most timeout milliseconds.
     * @param prefix the key prefix
     * @param count the expected count of values to be get
     * @param timeout the max wait time
     * @param logInterval the interval in ms to log message
     * @return the list of values which key with specified prefix
     */
    public List<byte[]> getWithPrefix(String prefix, int count,
                                      long timeout, long logInterval) {
        E.checkArgumentNotNull(prefix, "The prefix can't be null");
        E.checkArgument(count >= 0,
                        "The count must be >= 0, but got: %s", count);
        long deadline = System.currentTimeMillis() + timeout;
        List<byte[]> result = new ArrayList<>(count);
        ByteSequence prefixSeq = ByteSequence.from(prefix, ENCODING);
        GetOption getOption = GetOption.newBuilder().withPrefix(prefixSeq)
                                       .withSortOrder(SortOrder.ASCEND)
                                       .withLimit(count)
                                       .build();
        while (System.currentTimeMillis() < deadline) {
            try {
                GetResponse response = this.kv.get(prefixSeq, getOption).get();
                if (response.getCount() == count) {
                    List<KeyValue> kvs = response.getKvs();
                    for (KeyValue kv : kvs) {
                        result.add(kv.getValue().getBytes());
                    }
                    return result;
                } else {
                    timeout = deadline - System.currentTimeMillis();
                    if (timeout > 0) {
                        long revision = response.getHeader().getRevision();
                        int diff = (int) (count - response.getCount());
                        this.waitAndPrefixGetFromPutEvent(prefixSeq, count,
                                                          diff, revision,
                                                          timeout, logInterval);
                    } else {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new ComputerException(
                          "Interrupted while getting with prefix='%s', " +
                          "count=%s, timeout=%s", e, prefix, count, timeout);
            } catch (ExecutionException e) {
                throw new ComputerException(
                          "Error while getting with prefix='%s', count=%s, " +
                          "timeout=%s", e, prefix, count, timeout);
            }
        }
        return this.getWithPrefix(prefix, count);
    }

    /**
     * Wait at most expected eventCount events triggered in timeout ms.
     * This method wait at most timeout ms regardless whether expected
     * eventCount events triggered.
     */
    private void waitAndPrefixGetFromPutEvent(ByteSequence prefixSeq, int count,
                                              int eventCount, long revision,
                                              long timeout, long logInterval)
                                              throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout;
        CountDownLatch countDownLatch = new CountDownLatch(eventCount);
        WatchOption watchOption = WatchOption.newBuilder()
                                             .withPrefix(prefixSeq)
                                             .withRevision(revision)
                                             .withNoDelete(true)
                                             .build();
        Consumer<WatchResponse> consumer = watchResponse -> {
            List<WatchEvent> events = watchResponse.getEvents();
            for (WatchEvent event : events) {
                /*
                 * This event may not accurate, it may put the
                 * same key multiple times.
                 */
                if (EventType.PUT.equals(event.getEventType())) {
                    countDownLatch.countDown();
                } else {
                    throw new ComputerException("Unexpected event type '%s'",
                                                event.getEventType());
                }
            }
        };
        Watch.Watcher watcher = this.watch.watch(prefixSeq,
                                                 watchOption,
                                                 consumer);
        long timeRemaining = deadline - System.currentTimeMillis();
        while (timeRemaining > 0) {
            logInterval = Math.max(timeRemaining, logInterval);
            if (countDownLatch.await(logInterval, TimeUnit.MILLISECONDS)) {
                break;
            } else {
                LOG.info("Only {} out of {} sub-nodes created",
                         count - countDownLatch.getCount(), count);
            }
            timeRemaining = deadline - System.currentTimeMillis();
        }
        watcher.close();
    }

    /**
     * @return 1 if deleted specified key, 0 if not found specified key
     * The deleted data can be get through revision, if revision is compacted,
     * throw exception "etcdserver: mvcc: required revision has been compacted".
     * @see <a href="https://etcd.io/docs/v3.4.0/op-guide/maintenance/">
     *      Maintenance</a>
     */
    public long delete(String key) {
        E.checkArgumentNotNull(key, "The key can't be null");
        ByteSequence keySeq = ByteSequence.from(key, ENCODING);
        try {
            DeleteResponse response = this.client.getKVClient().delete(keySeq)
                                                 .get();
            return response.getDeleted();
        } catch (InterruptedException e) {
            throw new ComputerException("Interrupted while deleting '%s'",
                                        e, key);
        } catch (ExecutionException e) {
            throw new ComputerException("Error while deleting '%s'", e, key);
        }
    }

    /**
     * @return the number of keys deleted
     */
    public long deleteWithPrefix(String prefix) {
        E.checkArgumentNotNull(prefix, "The prefix can't be null");
        ByteSequence prefixSeq = ByteSequence.from(prefix, ENCODING);
        DeleteOption deleteOption = DeleteOption.newBuilder()
                                                .withPrefix(prefixSeq).build();
        try {
            DeleteResponse response = this.client.getKVClient()
                                                 .delete(prefixSeq,
                                                         deleteOption)
                                                 .get();
            return response.getDeleted();
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while deleting with prefix '%s'", e, prefix);
        } catch (ExecutionException e) {
            throw new ComputerException(
                      "ExecutionException is thrown while deleting with " +
                      "prefix '%s'", e, prefix);
        }
    }

    public long deleteAllKvsInNamespace() {
        return this.deleteWithPrefix("");
    }

    public void close() {
        this.client.close();
    }

    @VisibleForTesting
    protected KV getKv() {
        return this.kv;
    }
}
