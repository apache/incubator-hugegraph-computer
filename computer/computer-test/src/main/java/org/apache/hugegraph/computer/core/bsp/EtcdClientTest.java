/*
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

package org.apache.hugegraph.computer.core.bsp;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;

public class EtcdClientTest {

    private static String ENDPOINTS = "http://localhost:2579";
    private static String NAMESPACE = "test_job_0001";
    private static String KEY_PREFIX = "/key";
    private static String KEY1 = "/key1";
    private static String KEY2 = "/key2";
    private static String NO_SUCH_KEY = "/no-such-key";
    private static byte[] VALUE1 = "value1".getBytes(StandardCharsets.UTF_8);
    private static byte[] VALUE2 = "value2".getBytes(StandardCharsets.UTF_8);

    private EtcdClient client;

    @Before
    public void setup() {
        this.client = new EtcdClient(ENDPOINTS, NAMESPACE);
    }

    @After
    public void tearDown() {
        this.client.deleteAllKvsInNamespace();
        this.client.close();
    }

    @Test
    public void testPut() {
        this.client.put(KEY1, VALUE1);
        byte[] bytes = this.client.get(KEY1);
        Assert.assertArrayEquals(VALUE1, bytes);
        this.client.delete(KEY1);
    }

    @Test
    public void testGet() {
        this.client.put(KEY1, VALUE1);
        byte[] bytes1 = this.client.get(KEY1);
        Assert.assertArrayEquals(VALUE1, bytes1);
        byte[] bytes2 = this.client.get(NO_SUCH_KEY);
        Assert.assertNull(bytes2);
        this.client.delete(KEY1);
    }

    @Test
    public void testGetByNotExistKey() {
        this.client.put(KEY1, VALUE1);
        byte[] bytes1 = this.client.get(KEY1);
        Assert.assertArrayEquals(VALUE1, bytes1);
        byte[] bytes2 = this.client.get(NO_SUCH_KEY);
        Assert.assertNull(bytes2);
        Assert.assertThrows(ComputerException.class, () -> {
            this.client.get(NO_SUCH_KEY, true);
        });
        this.client.delete(KEY1);
    }

    @Test
    public void testGetWithTimeout() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Runnable putThread = () -> {
            UnitTestBase.sleep(100L);
            this.client.put(KEY2, VALUE2);
            this.client.put(KEY1, VALUE1);
        };
        executorService.submit(putThread);
        byte[] bytes1 = this.client.get(KEY1, 1000L, 500L);
        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertArrayEquals(VALUE1, bytes1);
        Assert.assertThrows(ComputerException.class, () -> {
            this.client.get(NO_SUCH_KEY, 1000L, 500L);
        });
        this.client.delete(KEY1);
        this.client.delete(KEY2);
    }

    @Test
    public void testGetTimeoutThrowException() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Runnable putThread = () -> {
            UnitTestBase.sleep(1000L);
            this.client.put(KEY1, VALUE1);
        };
        executorService.submit(putThread);
        Assert.assertThrows(ComputerException.class, () -> {
            this.client.get(KEY1, 50L, 50L);
        });
        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        long deleteCount = this.client.delete(KEY1);
        Assert.assertEquals(1L, deleteCount);
    }

    @Test
    public void testGetWithTimeoutAndDisturbKey() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Runnable putThread = () -> {
            UnitTestBase.sleep(100L);
            // Should ignore event prefix with KEY1
            this.client.put(KEY1 + "abc", VALUE2);
            this.client.put(KEY1, VALUE1);
        };
        executorService.submit(putThread);
        byte[] bytes1 = this.client.get(KEY1, 1000L, 500L);
        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertArrayEquals(VALUE1, bytes1);
    }

    @Test
    public void testGetWithPrefix() {
        this.client.put(KEY1, VALUE1);
        this.client.put(KEY2, VALUE2);
        List<byte[]> valueList1  = this.client.getWithPrefix(KEY_PREFIX);
        Assert.assertEquals(2, valueList1.size());
        List<byte[]> valueList2 = this.client.getWithPrefix(NO_SUCH_KEY);
        Assert.assertEquals(0, valueList2.size());
        this.client.delete(KEY1);
        this.client.delete(KEY2);
    }

    @Test
    public void testGetWithPrefixAndCount() {
        this.client.put(KEY2, VALUE2);
        this.client.put(KEY1, VALUE1);
        List<byte[]> valueList1 = this.client.getWithPrefix(KEY_PREFIX, 2);
        Assert.assertEquals(2, valueList1.size());
        Assert.assertArrayEquals(VALUE1, valueList1.get(0));
        Assert.assertArrayEquals(VALUE2, valueList1.get(1));

        Assert.assertThrows(ComputerException.class, () -> {
            this.client.getWithPrefix(NO_SUCH_KEY, 1);
        });

        this.client.delete(KEY1);
        this.client.delete(KEY2);
    }

    @Test
    public void testGetWithPrefixAndTimeout() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Runnable putThread = () -> {
            UnitTestBase.sleep(100L);
            this.client.put(KEY1, VALUE1);
            this.client.put(KEY1, VALUE1);
            this.client.put(KEY2, VALUE2);
        };
        executorService.submit(putThread);
        List<byte[]> valueList1 = this.client.getWithPrefix(KEY_PREFIX, 2,
                                                            1000L, 200L);
        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertEquals(2, valueList1.size());
        Assert.assertArrayEquals(VALUE1, valueList1.get(0));
        Assert.assertArrayEquals(VALUE2, valueList1.get(1));

        Assert.assertThrows(ComputerException.class, () -> {
            this.client.getWithPrefix(KEY_PREFIX, 3, 1000L, 200L);
        });
        Assert.assertThrows(ComputerException.class, () -> {
            this.client.getWithPrefix(NO_SUCH_KEY, 1, 1000L, 200L);
        });
        this.client.delete(KEY1);
        this.client.delete(KEY2);
    }

    @Test
    public void testGetWithRevision()
                throws ExecutionException, InterruptedException {
        KV kv = this.client.getKv();
        ByteSequence key1Seq = ByteSequence.from(KEY1, StandardCharsets.UTF_8);
        ByteSequence value1Seq = ByteSequence.from(VALUE1);
        ByteSequence value2Seq = ByteSequence.from(VALUE2);
        PutResponse putResponse1 = kv.put(key1Seq, value1Seq).get();
        long revision1 = putResponse1.getHeader().getRevision();
        PutResponse putResponse2 = kv.put(key1Seq, value2Seq).get();
        long revision2 = putResponse2.getHeader().getRevision();
        long deleteCount1 = kv.delete(ByteSequence.from(KEY1,
                                                        StandardCharsets.UTF_8))
                              .get().getDeleted();
        Assert.assertEquals(1, deleteCount1);
        GetOption getOption1 = GetOption.newBuilder().withRevision(revision1)
                                        .build();
        GetResponse getResponse1 = kv.get(key1Seq, getOption1).get();
        Assert.assertEquals(value1Seq,
                            getResponse1.getKvs().get(0).getValue());
        GetOption getOption2 = GetOption.newBuilder().withRevision(revision2)
                                        .build();
        GetResponse getResponse2 = kv.get(key1Seq, getOption2).get();
        Assert.assertEquals(value2Seq,
                            getResponse2.getKvs().get(0).getValue());
    }

    @Test
    public void testDelete() {
        this.client.put(KEY1, VALUE1);
        long deleteCount1 = this.client.delete(KEY1);
        Assert.assertEquals(1L, deleteCount1);
        long deleteCount2 = this.client.delete(KEY1);
        Assert.assertEquals(0L, deleteCount2);
        byte[] value = this.client.get(KEY1);
        Assert.assertNull(value);
        long deleteCount3 = this.client.delete(NO_SUCH_KEY);
        Assert.assertEquals(0L, deleteCount3);
    }

    @Test
    public void testDeleteWithPrefix() {
        this.client.put(KEY1, VALUE1);
        this.client.put(KEY2, VALUE2);
        this.client.put(NO_SUCH_KEY, VALUE2);
        long deleteCount1 = this.client.deleteWithPrefix(KEY_PREFIX);
        Assert.assertEquals(2L, deleteCount1);
        long deleteCount2 = this.client.delete(NO_SUCH_KEY);
        Assert.assertEquals(1L, deleteCount2);
    }

    @Test
    public void testDeleteAllKvInNamespace() {
        this.client.put(KEY1, VALUE1);
        this.client.put(KEY2, VALUE2);
        long deleteCount1 = this.client.deleteAllKvsInNamespace();
        Assert.assertEquals(2L, deleteCount1);
        long deleteCount2 = this.client.delete(KEY1);
        Assert.assertEquals(0L, deleteCount2);
    }
}
