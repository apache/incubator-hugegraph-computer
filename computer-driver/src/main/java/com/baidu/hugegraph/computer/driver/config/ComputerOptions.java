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

    public static final DriverConfigOption<String>
            ALGORITHM_RESULT_CLASS =
            new DriverConfigOption<>(
                    "algorithm.result_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            ALGORITHM_MESSAGE_CLASS =
            new DriverConfigOption<>(
                    "algorithm.message_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String> INPUT_SOURCE_TYPE =
            new DriverConfigOption<>(
                    "input.source_type",
                    allowValues("hugegraph"),
                    String.class
            );

    public static final DriverConfigOption<Long> INPUT_SPLITS_SIZE =
            new DriverConfigOption<>(
                    "input.split_size",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Integer> INPUT_MAX_SPLITS =
            new DriverConfigOption<>(
                    "input.split_max_splits",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            INPUT_SPLIT_PAGE_SIZE =
            new DriverConfigOption<>(
                    "input.split_page_size",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String> INPUT_FILTER_CLASS =
            new DriverConfigOption<>(
                    "input.filter_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            INPUT_EDGE_DIRECTION = new DriverConfigOption<>(
                    "input.edge_direction",
                    allowValues("OUT", "IN", "BOTH"),
                    String.class
            );

    public static final DriverConfigOption<String> INPUT_EDGE_FREQ =
            new DriverConfigOption<>(
                    "input.edge_freq",
                    allowValues("SINGLE", "SINGLE_PER_LABEL", "MULTIPLE"),
                    String.class
            );

    public static final DriverConfigOption<Integer>
            INPUT_MAX_EDGES_IN_ONE_VERTEX = new DriverConfigOption<>(
                    "input.max_edges_in_one_vertex",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> SORT_THREAD_NUMS =
            new DriverConfigOption<>(
                    "sort.thread_nums",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String> OUTPUT_RESULT_NAME =
            new DriverConfigOption<>(
                    "output.result_name",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Boolean>
            OUTPUT_WITH_ADJACENT_EDGES =
            new DriverConfigOption<>(
                    "output.with_adjacent_edges",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final DriverConfigOption<Boolean>
            OUTPUT_WITH_VERTEX_PROPERTIES = new DriverConfigOption<>(
                    "output.with_vertex_properties",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final DriverConfigOption<Boolean>
            OUTPUT_WITH_EDGE_PROPERTIES = new DriverConfigOption<>(
                    "output.with_edge_properties",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final DriverConfigOption<Integer> OUTPUT_BATCH_SIZE =
            new DriverConfigOption<>(
                    "output.batch_size",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> OUTPUT_BATCH_THREADS =
            new DriverConfigOption<>(
                    "output.batch_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> OUTPUT_SINGLE_THREADS =
            new DriverConfigOption<>(
                    "output.single_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            OUTPUT_THREAD_POOL_SHUTDOWN_TIMEOUT = new DriverConfigOption<>(
                    "output.thread_pool_shutdown_timeout",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> OUTPUT_RETRY_TIMES =
            new DriverConfigOption<>(
                    "output.retry_times",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> OUTPUT_RETRY_INTERVAL =
            new DriverConfigOption<>(
                    "output.retry_interval",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            VERTEX_AVERAGE_DEGREE =
            new DriverConfigOption<>(
                    "computer.vertex_average_degree",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
           ALLOCATOR_MAX_VERTICES_PER_THREAD = new DriverConfigOption<>(
                    "allocator.max_vertices_per_thread",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String> JOB_ID =
            new DriverConfigOption<>(
                    "job.id",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Integer> JOB_WORKERS_COUNT =
            new DriverConfigOption<>(
                    "job.workers_count",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            JOB_PARTITIONS_COUNT = new DriverConfigOption<>(
                    "job.partitions_count",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> BSP_MAX_SUPER_STEP =
            new DriverConfigOption<>(
                    "bsp.max_super_step",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String> BSP_ETCD_ENDPOINTS =
            new DriverConfigOption<>(
                    "bsp.etcd_endpoints",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Long> BSP_REGISTER_TIMEOUT =
            new DriverConfigOption<>(
                    "bsp.register_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            BSP_WAIT_WORKERS_TIMEOUT =
            new DriverConfigOption<>(
                    "bsp.wait_workers_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            BSP_WAIT_MASTER_TIMEOUT = new DriverConfigOption<>(
                    "bsp.wait_master_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long> BSP_LOG_INTERVAL =
            new DriverConfigOption<>(
                    "bsp.log_interval",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<String> WORKER_PARTITIONER =
            new DriverConfigOption<>(
                    "worker.partitioner",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            WORKER_COMPUTATION_CLASS =
            new DriverConfigOption<>(
                    "worker.computation_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            ALGORITHM_PARAMS_CLASS = new DriverConfigOption<>(
                    "algorithm.params_class",
                    "The class that set algorithms's parameters before " +
                    "algorithm been run.",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            WORKER_COMBINER_CLASS = new DriverConfigOption<>(
                    "worker.combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            WORKER_VERTEX_PROPERTIES_COMBINER_CLASS =
            new DriverConfigOption<>(
                    "worker.vertex_properties_combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            WORKER_EDGE_PROPERTIES_COMBINER_CLASS =
            new DriverConfigOption<>(
                    "worker.edge_properties_combiner_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Long>
            WORKER_RECEIVED_BUFFERS_BYTES_LIMIT =
            new DriverConfigOption<>(
                    "worker.received_buffers_bytes_limit",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            WORKER_WAIT_SORT_TIMEOUT =
            new DriverConfigOption<>(
                    "worker.wait_sort_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            WORKER_WAIT_FINISH_MESSAGES_TIMEOUT =
            new DriverConfigOption<>(
                    "worker.wait_finish_messages_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<String> WORKER_DATA_DIRS =
            new DriverConfigOption<>(
                    "worker.data_dirs",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Integer>
            WORKER_WRITE_BUFFER_THRESHOLD = new DriverConfigOption<>(
                    "worker.write_buffer_threshold",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            WORKER_WRITE_BUFFER_INIT_CAPACITY =
            new DriverConfigOption<>(
                    "worker.write_buffer_capacity",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String>
            MASTER_COMPUTATION_CLASS =
            new DriverConfigOption<>(
                    "master.computation_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String> HUGEGRAPH_URL =
            new DriverConfigOption<>(
                    "hugegraph.url",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            HUGEGRAPH_GRAPH_NAME = new DriverConfigOption<>(
                    "hugegraph.name",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String>
            TRANSPORT_SERVER_HOST = new DriverConfigOption<>(
                    "transport.server_host",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_SERVER_PORT =
            new DriverConfigOption<>(
                    "transport.server_port",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_SERVER_THREADS = new DriverConfigOption<>(
                    "transport.server_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_CLIENT_THREADS = new DriverConfigOption<>(
                    "transport.client_threads",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String>
            TRANSPORT_PROVIDER_CLASS = new DriverConfigOption<>(
                    "transport.provider_class",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String> TRANSPORT_IO_MODE =
            new DriverConfigOption<>(
                    "transport.io_mode",
                    allowValues("NIO", "EPOLL", "AUTO"),
                    String.class
            );

    public static final DriverConfigOption<Boolean> TRANSPORT_EPOLL_LT =
            new DriverConfigOption<>(
                    "transport.transport_epoll_lt",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final DriverConfigOption<Boolean>
            TRANSPORT_TCP_KEEP_ALIVE = new DriverConfigOption<>(
                    "transport.transport_tcp_keep_alive",
                    allowValues(true, false),
                    Boolean.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_MAX_SYN_BACKLOG = new DriverConfigOption<>(
                    "transport.max_syn_backlog",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_RECEIVE_BUFFER_SIZE = new DriverConfigOption<>(
                    "transport.receive_buffer_size",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_SEND_BUFFER_SIZE =
            new DriverConfigOption<>(
                    "transport.send_buffer_size",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_CLIENT_CONNECT_TIMEOUT = new DriverConfigOption<>(
                    "transport.client_connect_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_CLOSE_TIMEOUT = new DriverConfigOption<>(
                    "transport.close_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_SYNC_REQUEST_TIMEOUT = new DriverConfigOption<>(
                    "transport.sync_request_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_FINISH_SESSION_TIMEOUT = new DriverConfigOption<>(
                    "transport.finish_session_timeout",
                    nonNegativeInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_WRITE_SOCKET_TIMEOUT = new DriverConfigOption<>(
                    "transport.write_socket_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_NETWORK_RETRIES = new DriverConfigOption<>(
                    "transport.network_retries",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_WRITE_BUFFER_HIGH_MARK = new DriverConfigOption<>(
                    "transport.write_buffer_high_mark",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_WRITE_BUFFER_LOW_MARK = new DriverConfigOption<>(
                    "transport.write_buffer_low_mark",
                    nonNegativeInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_MAX_PENDING_REQUESTS = new DriverConfigOption<>(
                    "transport.max_pending_requests",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_MIN_PENDING_REQUESTS = new DriverConfigOption<>(
                    "transport.min_pending_requests",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_MIN_ACK_INTERVAL =
            new DriverConfigOption<>(
                    "transport.min_ack_interval",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_SERVER_IDLE_TIMEOUT = new DriverConfigOption<>(
                    "transport.server_idle_timeout",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long>
            TRANSPORT_HEARTBEAT_INTERVAL = new DriverConfigOption<>(
                    "transport.heartbeat_interval",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Integer>
            TRANSPORT_MAX_TIMEOUT_HEARTBEAT_COUNT =
            new DriverConfigOption<>(
                    "transport.max_timeout_heartbeat_count",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<Long> HGKV_MAX_FILE_SIZE =
            new DriverConfigOption<>(
                    "hgkv.max_file_size",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Long> HGKV_DATABLOCK_SIZE =
            new DriverConfigOption<>(
                    "hgkv.max_data_block_size",
                    positiveInt(),
                    Long.class
            );

    public static final DriverConfigOption<Integer>
            HGKV_MERGE_FILES_NUM = new DriverConfigOption<>(
                    "hgkv.max_merge_files",
                    positiveInt(),
                    Integer.class
            );

    public static final DriverConfigOption<String> HGKV_TEMP_DIR =
            new DriverConfigOption<>(
                    "hgkv.temp_file_dir",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String> RPC_SERVER_HOST =
            new DriverConfigOption<>(
                    "rpc.server_host",
                    null,
                    String.class
            );

    public static final DriverConfigOption<Integer> RPC_SERVER_PORT =
            new DriverConfigOption<>(
                    "rpc.server_port",
                    rangeInt(0, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<String> RPC_REMOTE_URL =
            new DriverConfigOption<>(
                    "rpc.remote_url",
                    null,
                    String.class
            );

    public static final DriverConfigOption<Boolean> RPC_ADAPTIVE_PORT =
            new DriverConfigOption<>(
                    "rpc.server_adaptive_port",
                    disallowEmpty(),
                    Boolean.class
            );

    public static final DriverConfigOption<Integer> RPC_SERVER_TIMEOUT =
            new DriverConfigOption<>(
                    "rpc.server_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            RPC_CLIENT_CONNECT_TIMEOUT = new DriverConfigOption<>(
                    "rpc.client_connect_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            RPC_CLIENT_RECONNECT_PERIOD = new DriverConfigOption<>(
                    "rpc.client_reconnect_period",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<Integer>
            RPC_CLIENT_READ_TIMEOUT =
            new DriverConfigOption<>(
                    "rpc.client_read_timeout",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<Integer> RPC_CLIENT_RETRIES =
            new DriverConfigOption<>(
                    "rpc.client_retries",
                    rangeInt(0, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<String>
            RPC_CLIENT_LOAD_BALANCER =
            new DriverConfigOption<>(
                    "rpc.client_load_balancer",
                    allowValues("random", "localPref", "roundRobin",
                                "consistentHash", "weightRoundRobin"),
                    String.class
            );

    public static final DriverConfigOption<String> RPC_PROTOCOL =
            new DriverConfigOption<>(
                    "rpc.protocol",
                    allowValues("bolt", "rest", "dubbo", "h2c", "http"),
                    String.class
            );

    public static final DriverConfigOption<Integer> RPC_CONFIG_ORDER =
            new DriverConfigOption<>(
                    "rpc.config_order",
                    rangeInt(1, Integer.MAX_VALUE),
                    Integer.class
            );

    public static final DriverConfigOption<String> RPC_LOGGER_IMPL =
            new DriverConfigOption<>(
                    "rpc.logger_impl",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<Long>
            VALUE_FILE_MAX_SEGMENT_SIZE =
            new DriverConfigOption<>(
                    "valuefile.max_segment_size",
                    positiveInt(),
                    Long.class
            );

    public static final Set<String> REQUIRED_INIT_OPTIONS = ImmutableSet.of(
            HUGEGRAPH_URL.name()
    );

    public static final Set<String> K8S_PROHIBIT_USER_SETTINGS =
            ImmutableSet.of(
                    HUGEGRAPH_URL.name(),
                    BSP_ETCD_ENDPOINTS.name(),
                    RPC_SERVER_HOST.name(),
                    TRANSPORT_SERVER_HOST.name(),
                    JOB_ID.name(),
                    JOB_WORKERS_COUNT.name(),
                    RPC_REMOTE_URL.name()
            );

    public static final Set<String> K8S_REQUIRED_USER_OPTIONS = ImmutableSet.of(
            ALGORITHM_PARAMS_CLASS.name()
    );
}
