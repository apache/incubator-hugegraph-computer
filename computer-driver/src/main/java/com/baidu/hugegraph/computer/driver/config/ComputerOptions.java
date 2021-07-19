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

package com.baidu.hugegraph.computer.driver.config;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.nonNegativeInt;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import java.util.Set;

import com.baidu.hugegraph.config.OptionHolder;
import com.google.common.collect.ImmutableSet;

public class ComputerOptions extends OptionHolder {

    private ComputerOptions() {
        super();
    }

    private static volatile ComputerOptions INSTANCE;

    public static synchronized ComputerOptions instance() {
        if (INSTANCE == null) {
            INSTANCE = new ComputerOptions();
            // Should initialize all static members first, then register.
            INSTANCE.registerOptions();
        }
        return INSTANCE;
    }

    public static final WithoutDefaultConfigOption<String>
            ALGORITHM_RESULT_CLASS =
            new WithoutDefaultConfigOption<>(
                    "algorithm.result_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String>
            ALGORITHM_MESSAGE_CLASS =
            new WithoutDefaultConfigOption<>(
                    "algorithm.message_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> INPUT_SOURCE_TYPE =
            new WithoutDefaultConfigOption<>(
                    "input.source_type",
                    allowValues("hugegraph"),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Long> INPUT_SPLITS_SIZE =
            new WithoutDefaultConfigOption<>(
                    "input.split_size",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Integer> INPUT_MAX_SPLITS =
            new WithoutDefaultConfigOption<>(
                    "input.split_max_splits",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            INPUT_SPLIT_PAGE_SIZE =
            new WithoutDefaultConfigOption<>(
                    "input.split_page_size",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String> INPUT_FILTER_CLASS =
            new WithoutDefaultConfigOption<>(
                    "input.filter_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> INPUT_EDGE_DIRECTION =
            new WithoutDefaultConfigOption<>(
                    "input.edge_direction",
                    allowValues("OUT", "IN", "BOTH"),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> INPUT_EDGE_FREQ =
            new WithoutDefaultConfigOption<>(
                    "input.edge_freq",
                    allowValues("SINGLE", "SINGLE_PER_LABEL", "MULTIPLE"),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            INPUT_MAX_EDGES_IN_ONE_VERTEX = new WithoutDefaultConfigOption<>(
                    "input.max_edges_in_one_vertex",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer> SORT_THREAD_NUMS =
            new WithoutDefaultConfigOption<>(
                    "sort.thread_nums",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String> OUTPUT_RESULT_NAME =
            new WithoutDefaultConfigOption<>(
                    "output.result_name",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Boolean>
            OUTPUT_WITH_ADJACENT_EDGES =
            new WithoutDefaultConfigOption<>(
                    "output.with_adjacent_edges",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final WithoutDefaultConfigOption<Boolean>
            OUTPUT_WITH_VERTEX_PROPERTIES = new WithoutDefaultConfigOption<>(
                    "output.with_vertex_properties",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final WithoutDefaultConfigOption<Boolean>
            OUTPUT_WITH_EDGE_PROPERTIES = new WithoutDefaultConfigOption<>(
                    "output.with_edge_properties",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            VERTEX_AVERAGE_DEGREE =
            new WithoutDefaultConfigOption<>(
                    "computer.vertex_average_degree",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
           ALLOCATOR_MAX_VERTICES_PER_THREAD = new WithoutDefaultConfigOption<>(
                    "allocator.max_vertices_per_thread",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String> JOB_ID =
            new WithoutDefaultConfigOption<>(
                    "job.id",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Integer> JOB_WORKERS_COUNT =
            new WithoutDefaultConfigOption<>(
                    "job.workers_count",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer> JOB_PARTITIONS_COUNT =
            new WithoutDefaultConfigOption<>(
                    "job.partitions_count",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer> BSP_MAX_SUPER_STEP =
            new WithoutDefaultConfigOption<>(
                    "bsp.max_super_step",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String> BSP_ETCD_ENDPOINTS =
            new WithoutDefaultConfigOption<>(
                    "bsp.etcd_endpoints",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Long> BSP_REGISTER_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "bsp.register_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            BSP_WAIT_WORKERS_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "bsp.wait_workers_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long> BSP_WAIT_MASTER_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "bsp.wait_master_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long> BSP_LOG_INTERVAL =
            new WithoutDefaultConfigOption<>(
                    "bsp.log_interval",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<String> WORKER_PARTITIONER =
            new WithoutDefaultConfigOption<>(
                    "worker.partitioner",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String>
            WORKER_COMPUTATION_CLASS =
            new WithoutDefaultConfigOption<>(
                    "worker.computation_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> WORKER_COMBINER_CLASS =
            new WithoutDefaultConfigOption<>(
                    "worker.combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String>
            WORKER_VERTEX_PROPERTIES_COMBINER_CLASS =
            new WithoutDefaultConfigOption<>(
                    "worker.vertex_properties_combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String>
            WORKER_EDGE_PROPERTIES_COMBINER_CLASS =
            new WithoutDefaultConfigOption<>(
                    "worker.edge_properties_combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Long>
            WORKER_RECEIVED_BUFFERS_BYTES_LIMIT = new WithoutDefaultConfigOption<>(
                    "worker.received_buffers_bytes_limit",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            WORKER_WAIT_SORT_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "worker.wait_sort_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            WORKER_WAIT_FINISH_MESSAGES_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "worker.wait_finish_messages_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<String> WORKER_DATA_DIRS =
            new WithoutDefaultConfigOption<>(
                    "worker.data_dirs",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            WORKER_WRITE_BUFFER_THRESHOLD = new WithoutDefaultConfigOption<>(
                    "worker.write_buffer_threshold",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            WORKER_WRITE_BUFFER_INIT_CAPACITY = new WithoutDefaultConfigOption<>(
                    "worker.write_buffer_capacity",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String>
            MASTER_COMPUTATION_CLASS =
            new WithoutDefaultConfigOption<>(
                    "master.computation_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> HUGEGRAPH_URL =
            new WithoutDefaultConfigOption<>(
                    "hugegraph.url",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> HUGEGRAPH_GRAPH_NAME =
            new WithoutDefaultConfigOption<>(
                    "hugegraph.name",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> TRANSPORT_SERVER_HOST =
            new WithoutDefaultConfigOption<>(
                    "transport.server_host",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_SERVER_PORT =
            new WithoutDefaultConfigOption<>(
                    "transport.server_port",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_SERVER_THREADS = new WithoutDefaultConfigOption<>(
                    "transport.server_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_CLIENT_THREADS = new WithoutDefaultConfigOption<>(
                    "transport.client_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String>
            TRANSPORT_PROVIDER_CLASS = new WithoutDefaultConfigOption<>(
                    "transport.provider_class",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> TRANSPORT_IO_MODE =
            new WithoutDefaultConfigOption<>(
                    "transport.io_mode",
                    allowValues("NIO", "EPOLL", "AUTO"),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Boolean> TRANSPORT_EPOLL_LT =
            new WithoutDefaultConfigOption<>(
                    "transport.transport_epoll_lt",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final WithoutDefaultConfigOption<Boolean>
            TRANSPORT_TCP_KEEP_ALIVE = new WithoutDefaultConfigOption<>(
                    "transport.transport_tcp_keep_alive",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_MAX_SYN_BACKLOG = new WithoutDefaultConfigOption<>(
                    "transport.max_syn_backlog",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_RECEIVE_BUFFER_SIZE = new WithoutDefaultConfigOption<>(
                    "transport.receive_buffer_size",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_SEND_BUFFER_SIZE =
            new WithoutDefaultConfigOption<>(
                    "transport.send_buffer_size",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_CLIENT_CONNECT_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "transport.client_connect_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long> TRANSPORT_CLOSE_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "transport.close_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_SYNC_REQUEST_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "transport.sync_request_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_FINISH_SESSION_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "transport.finish_session_timeout",
                    nonNegativeInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_WRITE_SOCKET_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "transport.write_socket_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_NETWORK_RETRIES = new WithoutDefaultConfigOption<>(
                    "transport.network_retries",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_WRITE_BUFFER_HIGH_MARK = new WithoutDefaultConfigOption<>(
                    "transport.write_buffer_high_mark",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_WRITE_BUFFER_LOW_MARK = new WithoutDefaultConfigOption<>(
                    "transport.write_buffer_low_mark",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_MAX_PENDING_REQUESTS = new WithoutDefaultConfigOption<>(
                    "transport.max_pending_requests",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_MIN_PENDING_REQUESTS = new WithoutDefaultConfigOption<>(
                    "transport.min_pending_requests",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_MIN_ACK_INTERVAL =
            new WithoutDefaultConfigOption<>(
                    "transport.min_ack_interval",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_SERVER_IDLE_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "transport.server_idle_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long>
            TRANSPORT_HEARTBEAT_INTERVAL = new WithoutDefaultConfigOption<>(
                    "transport.heartbeat_interval",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            TRANSPORT_MAX_TIMEOUT_HEARTBEAT_COUNT =
            new WithoutDefaultConfigOption<>(
                    "transport.max_timeout_heartbeat_count",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Long> HGKV_MAX_FILE_SIZE =
            new WithoutDefaultConfigOption<>(
                    "hgkv.max_file_size",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Long> HGKV_DATABLOCK_SIZE =
            new WithoutDefaultConfigOption<>(
                    "hgkv.max_data_block_size",
                    positiveInt(),
                    Long.class
            );

    public static final WithoutDefaultConfigOption<Integer> HGKV_MERGE_FILES_NUM =
            new WithoutDefaultConfigOption<>(
                    "hgkv.max_merge_files",
                    positiveInt(),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String> HGKV_TEMP_DIR =
            new WithoutDefaultConfigOption<>(
                    "hgkv.temp_file_dir",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> RPC_SERVER_HOST =
            new WithoutDefaultConfigOption<>(
                    "rpc.server_host",
                    null,
                    String.class
            );

    public static final WithoutDefaultConfigOption<Integer> RPC_SERVER_PORT =
            new WithoutDefaultConfigOption<>(
                    "rpc.server_port",
                    rangeInt(0, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Boolean> RPC_ADAPTIVE_PORT =
            new WithoutDefaultConfigOption<>(
                    "rpc.server_adaptive_port",
                    disallowEmpty(),
                    Boolean.class
            );

    public static final WithoutDefaultConfigOption<Integer> RPC_SERVER_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "rpc.server_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            RPC_CLIENT_CONNECT_TIMEOUT = new WithoutDefaultConfigOption<>(
                    "rpc.client_connect_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            RPC_CLIENT_RECONNECT_PERIOD = new WithoutDefaultConfigOption<>(
                    "rpc.client_reconnect_period",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer>
            RPC_CLIENT_READ_TIMEOUT =
            new WithoutDefaultConfigOption<>(
                    "rpc.client_read_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<Integer> RPC_CLIENT_RETRIES =
            new WithoutDefaultConfigOption<>(
                    "rpc.client_retries",
                    rangeInt(0, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String>
            RPC_CLIENT_LOAD_BALANCER =
            new WithoutDefaultConfigOption<>(
                    "rpc.client_load_balancer",
                    allowValues("random", "localPref", "roundRobin",
                                "consistentHash", "weightRoundRobin"),
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> RPC_PROTOCOL =
            new WithoutDefaultConfigOption<>(
                    "rpc.protocol",
                    allowValues("bolt", "rest", "dubbo", "h2c", "http"),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Integer> RPC_CONFIG_ORDER =
            new WithoutDefaultConfigOption<>(
                    "rpc.config_order",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final WithoutDefaultConfigOption<String> RPC_LOGGER_IMPL =
            new WithoutDefaultConfigOption<>(
                    "rpc.logger_impl",
                    disallowEmpty(),
                    String.class
            );

    public static final WithoutDefaultConfigOption<Long>
            VALUE_FILE_MAX_SEGMENT_SIZE =
            new WithoutDefaultConfigOption<>(
                    "valuefile.max_segment_size",
                    positiveInt(),
                    Long.class
            );

    public static Set<String> PROHIBIT_USER_SETTINGS = ImmutableSet.of(
            HUGEGRAPH_URL.name(),
            BSP_ETCD_ENDPOINTS.name(),
            RPC_SERVER_HOST.name(),
            TRANSPORT_SERVER_HOST.name(),
            JOB_WORKERS_COUNT.name(),
            JOB_ID.name()
    );

    public static Set<String> REQUIRED_OPTIONS = ImmutableSet.of(
            HUGEGRAPH_URL.name(),
            BSP_ETCD_ENDPOINTS.name()
    );
}
