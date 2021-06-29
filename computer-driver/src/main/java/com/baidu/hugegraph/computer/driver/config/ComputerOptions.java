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

    public static final NoDefaultConfigOption<String> ALGORITHM_RESULT_CLASS =
            new NoDefaultConfigOption<>(
                    "algorithm.result_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> ALGORITHM_MESSAGE_CLASS =
            new NoDefaultConfigOption<>(
                    "algorithm.message_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> INPUT_SOURCE_TYPE =
            new NoDefaultConfigOption<>(
                    "input.source_type",
                    allowValues("hugegraph"),
                    String.class
            );

    public static final NoDefaultConfigOption<Long> INPUT_SPLITS_SIZE =
            new NoDefaultConfigOption<>(
                    "input.split_size",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Integer> INPUT_MAX_SPLITS =
            new NoDefaultConfigOption<>(
                    "input.split_max_splits",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer> INPUT_SPLIT_PAGE_SIZE =
            new NoDefaultConfigOption<>(
                    "input.split_page_size",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> INPUT_FILTER_CLASS =
            new NoDefaultConfigOption<>(
                    "input.filter_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> INPUT_EDGE_DIRECTION =
            new NoDefaultConfigOption<>(
                    "input.edge_direction",
                    allowValues("OUT", "IN", "BOTH"),
                    String.class
            );

    public static final NoDefaultConfigOption<String> INPUT_EDGE_FREQ =
            new NoDefaultConfigOption<>(
                    "input.edge_freq",
                    allowValues("SINGLE", "SINGLE_PER_LABEL", "MULTIPLE"),
                    String.class
            );

    public static final NoDefaultConfigOption<Integer>
            INPUT_MAX_EDGES_IN_ONE_VERTEX = new NoDefaultConfigOption<>(
                    "input.max_edges_in_one_vertex",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer> SORT_THREAD_NUMS =
            new NoDefaultConfigOption<>(
                    "sort.thread_nums",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> OUTPUT_RESULT_NAME =
            new NoDefaultConfigOption<>(
                    "output.result_name",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<Boolean>
            OUTPUT_WITH_ADJACENT_EDGES =
            new NoDefaultConfigOption<>(
                    "output.with_adjacent_edges",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final NoDefaultConfigOption<Boolean>
            OUTPUT_WITH_VERTEX_PROPERTIES = new NoDefaultConfigOption<>(
                    "output.with_vertex_properties",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final NoDefaultConfigOption<Boolean>
            OUTPUT_WITH_EDGE_PROPERTIES = new NoDefaultConfigOption<>(
                    "output.with_edge_properties",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final NoDefaultConfigOption<Integer> VERTEX_AVERAGE_DEGREE =
            new NoDefaultConfigOption<>(
                    "computer.vertex_average_degree",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
           ALLOCATOR_MAX_VERTICES_PER_THREAD = new NoDefaultConfigOption<>(
                    "allocator.max_vertices_per_thread",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> JOB_ID =
            new NoDefaultConfigOption<>(
                    "job.id",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<Integer> JOB_WORKERS_COUNT =
            new NoDefaultConfigOption<>(
                    "job.workers_count",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer> JOB_PARTITIONS_COUNT =
            new NoDefaultConfigOption<>(
                    "job.partitions_count",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer> BSP_MAX_SUPER_STEP =
            new NoDefaultConfigOption<>(
                    "bsp.max_super_step",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> BSP_ETCD_ENDPOINTS =
            new NoDefaultConfigOption<>(
                    "bsp.etcd_endpoints",
                    true,
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<Long> BSP_REGISTER_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "bsp.register_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long> BSP_WAIT_WORKERS_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "bsp.wait_workers_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long> BSP_WAIT_MASTER_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "bsp.wait_master_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long> BSP_LOG_INTERVAL =
            new NoDefaultConfigOption<>(
                    "bsp.log_interval",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<String> WORKER_PARTITIONER =
            new NoDefaultConfigOption<>(
                    "worker.partitioner",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> WORKER_COMPUTATION_CLASS =
            new NoDefaultConfigOption<>(
                    "worker.computation_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> WORKER_COMBINER_CLASS =
            new NoDefaultConfigOption<>(
                    "worker.combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String>
            WORKER_VERTEX_PROPERTIES_COMBINER_CLASS =
            new NoDefaultConfigOption<>(
                    "worker.vertex_properties_combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String>
            WORKER_EDGE_PROPERTIES_COMBINER_CLASS =
            new NoDefaultConfigOption<>(
                    "worker.edge_properties_combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<Long>
            WORKER_RECEIVED_BUFFERS_BYTES_LIMIT = new NoDefaultConfigOption<>(
                    "worker.received_buffers_bytes_limit",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long> WORKER_WAIT_SORT_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "worker.wait_sort_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long>
            WORKER_WAIT_FINISH_MESSAGES_TIMEOUT = new NoDefaultConfigOption<>(
                    "worker.wait_finish_messages_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<String> WORKER_DATA_DIRS =
            new NoDefaultConfigOption<>(
                    "worker.data_dirs",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<Integer>
            WORKER_WRITE_BUFFER_THRESHOLD = new NoDefaultConfigOption<>(
                    "worker.write_buffer_threshold",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            WORKER_WRITE_BUFFER_INIT_CAPACITY = new NoDefaultConfigOption<>(
                    "worker.write_buffer_capacity",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> MASTER_COMPUTATION_CLASS =
            new NoDefaultConfigOption<>(
                    "master.computation_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> HUGEGRAPH_URL =
            new NoDefaultConfigOption<>(
                    "hugegraph.url",
                    true,
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> HUGEGRAPH_GRAPH_NAME =
            new NoDefaultConfigOption<>(
                    "hugegraph.name",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> TRANSPORT_SERVER_HOST =
            new NoDefaultConfigOption<>(
                    "transport.server_host",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<Integer> TRANSPORT_SERVER_PORT =
            new NoDefaultConfigOption<>(
                    "transport.server_port",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_SERVER_THREADS = new NoDefaultConfigOption<>(
                    "transport.server_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_CLIENT_THREADS = new NoDefaultConfigOption<>(
                    "transport.client_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String>
            TRANSPORT_PROVIDER_CLASS = new NoDefaultConfigOption<>(
                    "transport.provider_class",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> TRANSPORT_IO_MODE =
            new NoDefaultConfigOption<>(
                    "transport.io_mode",
                    allowValues("NIO", "EPOLL", "AUTO"),
                    String.class
            );

    public static final NoDefaultConfigOption<Boolean> TRANSPORT_EPOLL_LT =
            new NoDefaultConfigOption<>(
                    "transport.transport_epoll_lt",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final NoDefaultConfigOption<Boolean>
            TRANSPORT_TCP_KEEP_ALIVE = new NoDefaultConfigOption<>(
                    "transport.transport_tcp_keep_alive",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_MAX_SYN_BACKLOG = new NoDefaultConfigOption<>(
                    "transport.max_syn_backlog",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_RECEIVE_BUFFER_SIZE = new NoDefaultConfigOption<>(
                    "transport.receive_buffer_size",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_SEND_BUFFER_SIZE =
            new NoDefaultConfigOption<>(
                    "transport.send_buffer_size",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Long>
            TRANSPORT_CLIENT_CONNECT_TIMEOUT = new NoDefaultConfigOption<>(
                    "transport.client_connect_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long> TRANSPORT_CLOSE_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "transport.close_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long>
            TRANSPORT_SYNC_REQUEST_TIMEOUT = new NoDefaultConfigOption<>(
                    "transport.sync_request_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long>
            TRANSPORT_FINISH_SESSION_TIMEOUT = new NoDefaultConfigOption<>(
                    "transport.finish_session_timeout",
                    nonNegativeInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long>
            TRANSPORT_WRITE_SOCKET_TIMEOUT = new NoDefaultConfigOption<>(
                    "transport.write_socket_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_NETWORK_RETRIES = new NoDefaultConfigOption<>(
                    "transport.network_retries",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_WRITE_BUFFER_HIGH_MARK = new NoDefaultConfigOption<>(
                    "transport.write_buffer_high_mark",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_WRITE_BUFFER_LOW_MARK = new NoDefaultConfigOption<>(
                    "transport.write_buffer_low_mark",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_MAX_PENDING_REQUESTS = new NoDefaultConfigOption<>(
                    "transport.max_pending_requests",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_MIN_PENDING_REQUESTS = new NoDefaultConfigOption<>(
                    "transport.min_pending_requests",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Long> TRANSPORT_MIN_ACK_INTERVAL =
            new NoDefaultConfigOption<>(
                    "transport.min_ack_interval",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long>
            TRANSPORT_SERVER_IDLE_TIMEOUT = new NoDefaultConfigOption<>(
                    "transport.server_idle_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long>
            TRANSPORT_HEARTBEAT_INTERVAL = new NoDefaultConfigOption<>(
                    "transport.heartbeat_interval",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Integer>
            TRANSPORT_MAX_TIMEOUT_HEARTBEAT_COUNT =
            new NoDefaultConfigOption<>(
                    "transport.max_timeout_heartbeat_count",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Long> HGKV_MAX_FILE_SIZE =
            new NoDefaultConfigOption<>(
                    "hgkv.max_file_size",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Long> HGKV_DATABLOCK_SIZE =
            new NoDefaultConfigOption<>(
                    "hgkv.max_data_block_size",
                    positiveInt(),
                    Long.class
            );

    public static final NoDefaultConfigOption<Integer> HGKV_MERGE_FILES_NUM =
            new NoDefaultConfigOption<>(
                    "hgkv.max_merge_files",
                    positiveInt(),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> HGKV_TEMP_DIR =
            new NoDefaultConfigOption<>(
                    "hgkv.temp_file_dir",
                    disallowEmpty(),
                    String.class
            );

    public static final NoDefaultConfigOption<String> RPC_SERVER_HOST =
            new NoDefaultConfigOption<>(
                    "rpc.server_host",
                    null,
                    String.class
            );

    public static final NoDefaultConfigOption<Integer> RPC_SERVER_PORT =
            new NoDefaultConfigOption<>(
                    "rpc.server_port",
                    rangeInt(0, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Boolean> RPC_ADAPTIVE_PORT =
            new NoDefaultConfigOption<>(
                    "rpc.server_adaptive_port",
                    disallowEmpty(),
                    Boolean.class
            );

    public static final NoDefaultConfigOption<Integer> RPC_SERVER_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "rpc.server_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            RPC_CLIENT_CONNECT_TIMEOUT = new NoDefaultConfigOption<>(
                    "rpc.client_connect_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer>
            RPC_CLIENT_RECONNECT_PERIOD = new NoDefaultConfigOption<>(
                    "rpc.client_reconnect_period",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer> RPC_CLIENT_READ_TIMEOUT =
            new NoDefaultConfigOption<>(
                    "rpc.client_read_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<Integer> RPC_CLIENT_RETRIES =
            new NoDefaultConfigOption<>(
                    "rpc.client_retries",
                    rangeInt(0, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> RPC_CLIENT_LOAD_BALANCER =
            new NoDefaultConfigOption<>(
                    "rpc.client_load_balancer",
                    allowValues("random", "localPref", "roundRobin",
                                "consistentHash", "weightRoundRobin"),
                    String.class
            );

    public static final NoDefaultConfigOption<String> RPC_PROTOCOL =
            new NoDefaultConfigOption<>(
                    "rpc.protocol",
                    allowValues("bolt", "rest", "dubbo", "h2c", "http"),
                    String.class
            );

    public static final NoDefaultConfigOption<Integer> RPC_CONFIG_ORDER =
            new NoDefaultConfigOption<>(
                    "rpc.config_order",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final NoDefaultConfigOption<String> RPC_LOGGER_IMPL =
            new NoDefaultConfigOption<>(
                    "rpc.logger_impl",
                    disallowEmpty(),
                    String.class
            );

    public static Set<String> PROHIBIT_USER_SETTINGS = ImmutableSet.of(
            HUGEGRAPH_URL.name(),
            RPC_SERVER_HOST.name(),
            TRANSPORT_SERVER_HOST.name(),
            JOB_WORKERS_COUNT.name(),
            JOB_ID.name()
    );
}
