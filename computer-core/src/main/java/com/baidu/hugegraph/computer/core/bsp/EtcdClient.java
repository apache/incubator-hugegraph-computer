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

import static io.etcd.jetcd.options.GetOption.SortOrder.ASCEND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import com.baidu.hugegraph.computer.exception.ComputerException;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.E;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;

public class EtcdClient {

    private static WatchEvent.EventType PUT = WatchEvent.EventType.PUT;

    private final Client client;
    private final Watch watch;
    private final KV kv;

    public EtcdClient(String endpoints, String namespace) {
        E.checkArgumentNotNull(endpoints, "endpoints can't be null.");
        E.checkArgumentNotNull(endpoints, "namespace can't be null.");
        ByteSequence namespaceBs = ByteSequence.from(namespace.getBytes(UTF_8));
        this.client = Client.builder().endpoints(endpoints)
                            .namespace(namespaceBs).build();
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
        E.checkArgument(key != null, "Key can't be null.");
        E.checkArgument(value != null, "value can't be null.");
        try {
            this.kv.put(ByteSequence.from(key, UTF_8),
                        ByteSequence.from(value)).get();
        } catch (InterruptedException e) {
            String message = "Thread is interrupted in put key='%s'.";
            throw new ComputerException(message, e, key);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw in put key='%s'.";
            throw new ComputerException(message, e, key);
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
        E.checkArgumentNotNull(key, "Key can't be null.");
        try {
            ByteSequence keyBs = ByteSequence.from(key, UTF_8);
            GetResponse response = this.kv.get(keyBs).get();
            if (response.getCount() > 0) {
                List<KeyValue> kvs = response.getKvs();
                return kvs.get(0).getValue().getBytes();
            } else if (throwException) {
                 String message = "Can't find value for key='%s'.";
                 throw new ComputerException(message, key);
            }
            return null;
        } catch (InterruptedException e) {
            String message = "Thread is interrupted in get key='%s'.";
            throw new ComputerException(message, e, key);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw in get key='%s'.";
            throw new ComputerException(message, e, key);
        }
    }

    /**
     * Returns the value to which the specified key is mapped. If no
     * key exists, wait at most timeout milliseconds. Or throw
     * ComputerException if timeout and throwException is set
     * true.
     * @param key the key whose associated value is to be returned.
     * @param timeout the max time in milliseconds to wait.
     * @param throwException whether to throw ComputerException if not found.
     * @return the specified value in byte array to which the specified key is
     * mapped.
     */
    public byte[] get(String key, long timeout, boolean throwException) {
        E.checkArgumentNotNull(key, "Key can't be null.");
        E.checkArgument(timeout > 0L, "Timeout must be bigger than 0.");
        long deadline = System.currentTimeMillis() + timeout;
        ByteSequence keyBs = ByteSequence.from(key, UTF_8);
        try {
            GetResponse response = this.kv.get(keyBs).get();
            if (response.getCount() > 0) {
                List<KeyValue> kvs = response.getKvs();
                return kvs.get(0).getValue().getBytes();
            } else {
                long revision = response.getHeader().getRevision();
                final BarrierEvent barrierEvent = new BarrierEvent();
                WatchOption watchOption = WatchOption.newBuilder()
                                                     .withRevision(revision)
                                                     .withNoDelete(true)
                                                     .build();
                Consumer<WatchResponse> consumer = watchResponse -> {
                    List<WatchEvent> events = watchResponse.getEvents();
                    for (WatchEvent event : events) {
                        if (event.getEventType().equals(PUT)) {
                            barrierEvent.signalAll();
                        }
                    }
                };
                Watch.Watcher watcher = this.watch.watch(keyBs, watchOption,
                                                         consumer);
                timeout = deadline - System.currentTimeMillis();
                if (timeout > 0) {
                    barrierEvent.await(timeout);
                }
                watcher.close();
                return this.get(key, throwException);
            }
        } catch (InterruptedException e) {
            String message = "Thread is interrupted while get '%s'.";
            throw new ComputerException(message, e, key);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw while get '%s'.";
            throw new ComputerException(message, e, key);
        }
    }

    /**
     * Get the count of values of keys with the specified prefix.
     * If no key found, return empty list.
     */
    public List<byte[]> getWithPrefix(String prefix) {
        E.checkArgumentNotNull(prefix, "Prefix can't be null.");
        try {
            ByteSequence prefixBs = ByteSequence.from(prefix, UTF_8);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixBs)
                                           .withSortOrder(ASCEND).build();
            GetResponse response = this.kv.get(prefixBs, getOption).get();
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
            String message = "Thread is interrupted in getBytesList '%s'.";
            throw new ComputerException(message, e, prefix);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw in getBytesList " +
                             "'%s'.";
            throw new ComputerException(message, e, prefix);
        }
    }

    /**
     * Get the count of values of keys with the specified prefix.
     * Throws ComputerException if there are no enough object and throwException
     * is set true.
     */
    public List<byte[]> getWithPrefix(String prefix, int count,
                                      boolean throwException) {
        E.checkArgumentNotNull(prefix, "Prefix can't be null.");
        E.checkArgument(count >= 0,
                        "ExpectedCount must be bigger or equals 0.");
        try {
            ByteSequence prefixBs = ByteSequence.from(prefix, UTF_8);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixBs)
                                           .withLimit(count)
                                           .withSortOrder(ASCEND).build();
            GetResponse response = this.kv.get(prefixBs, getOption).get();
            if (response.getCount() == count || !throwException) {
                List<KeyValue> kvs = response.getKvs();
                List<byte[]> result = new ArrayList<>(kvs.size());
                for (KeyValue kv : kvs) {
                    result.add(kv.getValue().getBytes());
                }
                return result;
            } else {
                String message = "There are no '%d' elements, only find " +
                                 "'%d' elements with prefix='%s'.";
                throw new ComputerException(message, count,
                                            response.getCount(), prefix);
            }
        } catch (InterruptedException e) {
            String message = "Thread is interrupted in getBytesList " +
                             "prefix='%s', count='%d'.";
            throw new ComputerException(message, e, prefix, count);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw in getBytesList " +
                             "with prefix='%s', count='%d.";
            throw new ComputerException(message, e, prefix, count);
        }
    }

    /**
     * Get expected count of values with the key prefix with prefix. If there
     * is no count of keys, wait at most timeout milliseconds.
     * @param prefix the key prefix
     * @param count the expected count of values to be get
     * @param timeout the max wait time
     * @param throwException whether throwException when time is out and not
     * enough
     * kvs found.
     * @return the list of values which key with specified prefix
     */
    public List<byte[]> getWithPrefix(String prefix, int count,
                                      long timeout, boolean throwException) {
        E.checkArgumentNotNull(prefix, "Prefix can't be null.");
        E.checkArgument(count >= 0,
                        "Count must be bigger or equals 0.");
        long deadline = System.currentTimeMillis() + timeout;
        List<byte[]> result = new ArrayList<>(count);
        ByteSequence prefixBs = ByteSequence.from(prefix, UTF_8);
        GetOption getOption = GetOption.newBuilder().withPrefix(prefixBs)
                                       .withSortOrder(ASCEND).withLimit(count)
                                       .build();
        while (System.currentTimeMillis() < deadline) {
            try {
                GetResponse response = this.kv.get(prefixBs, getOption).get();
                if (response.getCount() == count) {
                    List<KeyValue> kvs = response.getKvs();
                    for (KeyValue kv : kvs) {
                        result.add(kv.getValue().getBytes());
                    }
                    return result;
                } else {
                    long revision = response.getHeader().getRevision();
                    int diff = (int) (count - response.getCount());
                    CountDownLatch countDownLatch = new CountDownLatch(diff);
                    WatchOption watchOption = WatchOption.newBuilder()
                                                         .withPrefix(prefixBs)
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
                            if (event.getEventType().equals(PUT)) {
                                countDownLatch.countDown();
                            }
                        }
                    };
                    Watch.Watcher watcher = this.watch.watch(prefixBs,
                                                             watchOption,
                                                             consumer);
                    timeout = deadline - System.currentTimeMillis();
                    if (timeout > 0) {
                        countDownLatch.await(timeout, MILLISECONDS);
                    }
                    watcher.close();
                }
            } catch (InterruptedException e) {
                String message = "Thread is interrupted in " +
                                 "getBytesListWithTimeout with prefix='%s', " +
                                 "count='%d', timeout='%d'.";
                throw new ComputerException(message, e, prefix, timeout);
            } catch (ExecutionException e) {
                String message = "ExecutionException is throw in " +
                                 "getBytesListWithTimeoutwith prefix='%s'," +
                                 "count='%d', timeout='%d'.";
                throw new ComputerException(message, e, prefix, timeout);
            }
        }
        return this.getWithPrefix(prefix, count, throwException);
    }

    public long delete(String key) {
        E.checkArgumentNotNull(key, "Key can't be null.");
        ByteSequence keyBs = ByteSequence.from(key, UTF_8);
        try {
            DeleteResponse response = this.client.getKVClient().delete(keyBs)
                                                 .get();
            return response.getDeleted();
        } catch (InterruptedException e) {
            String message = "Thread is interrupted while delete '%s'.";
            throw new ComputerException(message, e, key);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw while delete '%s'.";
            throw new ComputerException(message, e, key);
        }
    }

    public long deleteWithPrefix(String prefix) {
        E.checkArgumentNotNull(prefix, "Prefix can't be null.");
        ByteSequence prefixBs = ByteSequence.from(prefix, UTF_8);
        DeleteOption deleteOption = DeleteOption.newBuilder()
                                                .withPrefix(prefixBs).build();
        try {
            DeleteResponse response = this.client.getKVClient()
                                                 .delete(prefixBs, deleteOption)
                                                 .get();
            return response.getDeleted();
        } catch (InterruptedException e) {
            String message = "Thread is interrupted in deleteWithPrefix '%s'.";
            throw new ComputerException(message, e, prefix);
        } catch (ExecutionException e) {
            String message = "ExecutionException is throw.";
            throw new ComputerException(message, e);
        }
    }

    public long deleteAllKvsInNamespace() {
        return deleteWithPrefix("");
    }

    public void close() {
        client.close();
    }
}
