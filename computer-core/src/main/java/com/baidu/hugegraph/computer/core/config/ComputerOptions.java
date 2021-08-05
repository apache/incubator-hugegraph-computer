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

package com.baidu.hugegraph.computer.core.config;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.nonNegativeInt;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.computer.core.combiner.OverwriteCombiner;
import com.baidu.hugegraph.computer.core.graph.partition.HashPartitioner;
import com.baidu.hugegraph.computer.core.input.DefaultInputFilter;
import com.baidu.hugegraph.computer.core.master.DefaultMasterComputation;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.netty.NettyTransportProvider;
import com.baidu.hugegraph.computer.core.output.LogOutput;
import com.baidu.hugegraph.config.ConfigConvOption;
import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.structure.constant.Direction;
import com.baidu.hugegraph.util.Bytes;
import com.google.common.collect.ImmutableList;
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

    public static final ConfigOption<Class<?>> ALGORITHM_PARAMS_CLASS =
            new ConfigOption<>(
                    "algorithm.params_class",
                    "The class that set algorithms's parameters before " +
                    "algorithm been run.",
                    disallowEmpty(),
                    Null.class
            );

    public static final ConfigOption<Class<?>> ALGORITHM_RESULT_CLASS =
            new ConfigOption<>(
                    "algorithm.result_class",
                    "The class of vertex's value, the instance is used to " +
                    "store computation result for the vertex.",
                    disallowEmpty(),
                    Null.class
            );

    public static final ConfigOption<Class<?>> ALGORITHM_MESSAGE_CLASS =
            new ConfigOption<>(
                    "algorithm.message_class",
                    "The class of message passed when compute vertex.",
                    disallowEmpty(),
                    Null.class
            );

    public static final ConfigOption<String> INPUT_SOURCE_TYPE =
            new ConfigOption<>(
                    "input.source_type",
                    "The source type to load input data",
                    allowValues("hugegraph"),
                    "hugegraph"
            );

    public static final ConfigOption<Long> INPUT_SPLITS_SIZE =
            new ConfigOption<>(
                    "input.split_size",
                    "The input split size in bytes",
                    positiveInt(),
                    1024 * 1024L
            );

    public static final ConfigOption<Integer> INPUT_MAX_SPLITS =
            new ConfigOption<>(
                    "input.split_max_splits",
                    "The maximum number of input splits",
                    positiveInt(),
                    10_000_000
            );

    public static final ConfigOption<Integer> INPUT_SPLIT_PAGE_SIZE =
            new ConfigOption<>(
                    "input.split_page_size",
                    "The page size for streamed load input split data",
                    positiveInt(),
                    500
            );

    public static final ConfigOption<Class<?>> INPUT_FILTER_CLASS =
            new ConfigOption<>(
                    "input.filter_class",
                    "The class to create input-filter object, " +
                    "input-filter is used to Filter vertex edges " +
                    "according to user needs.",
                    disallowEmpty(),
                    DefaultInputFilter.class
            );

    public static final ConfigConvOption<String, Direction>
            INPUT_EDGE_DIRECTION = new ConfigConvOption<>(
                    "input.edge_direction",
                    "The data of the edge in which direction is loaded, " +
                    "when the value is BOTH, the edges in both OUT and IN " +
                    "direction will be loaded.",
                    allowValues("OUT", "IN", "BOTH"),
                    Direction::valueOf,
                    "OUT"
            );

    public static final ConfigConvOption<String, EdgeFrequency>
            INPUT_EDGE_FREQ = new ConfigConvOption<>(
                    "input.edge_freq",
                    "The frequency of edges can exist between a pair of " +
                    "vertices, allowed values: [SINGLE, SINGLE_PER_LABEL, " +
                    "MULTIPLE]. SINGLE means that only one edge can exist " +
                    "between a pair of vertices, use sourceId + targetId to " +
                    "identify it; SINGLE_PER_LABEL means that each edge " +
                    "label can exist one edge between a pair of vertices, " +
                    "use sourceId + edgelabel + targetId to identify it; " +
                    "MULTIPLE means that many edge can exist between a pair " +
                    "of vertices, use sourceId + edgelabel + sortValues + " +
                    "targetId to identify it.",
                    allowValues("SINGLE", "SINGLE_PER_LABEL", "MULTIPLE"),
                    EdgeFrequency::valueOf,
                    "SINGLE"
            );

    public static final ConfigOption<Integer> INPUT_MAX_EDGES_IN_ONE_VERTEX =
            new ConfigOption<>(
                    "input.max_edges_in_one_vertex",
                    "The maximum number of adjacent edges allowed to be " +
                    "attached to a vertex, the adjacent edges will be " +
                    "stored and transferred together as a batch unit.",
                    positiveInt(),
                    200
            );

    public static final ConfigOption<Integer> SORT_THREAD_NUMS =
            new ConfigOption<>(
                    "sort.thread_nums",
                    "The number of threads performing internal sorting.",
                    positiveInt(),
                    4
            );

    public static final ConfigOption<Class<?>> OUTPUT_CLASS =
            new ConfigOption<>(
                    "output.output_class",
                    "The class to output the computation result of each " +
                    "vertex. Be called after iteration computation.",
                    disallowEmpty(),
                    LogOutput.class
            );

    public static final ConfigOption<String> OUTPUT_RESULT_NAME =
            new ConfigOption<>(
                    "output.result_name",
                    "The value is assigned dynamically by #name() of " +
                    "instance created by WORKER_COMPUTATION_CLASS.",
                    disallowEmpty(),
                    "value"
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_ADJACENT_EDGES =
            new ConfigOption<>(
                    "output.with_adjacent_edges",
                    "Output the adjacent edges of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_VERTEX_PROPERTIES =
            new ConfigOption<>(
                    "output.with_vertex_properties",
                    "Output the properties of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_EDGE_PROPERTIES =
            new ConfigOption<>(
                    "output.with_edge_properties",
                    "Output the properties of the edge or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Integer> VERTEX_AVERAGE_DEGREE =
            new ConfigOption<>(
                    "computer.vertex_average_degree",
                    "The average degree of a vertex, it represents the " +
                    "average number of adjacent edges per vertex",
                    positiveInt(),
                    10
            );

    public static final ConfigOption<Integer>
           ALLOCATOR_MAX_VERTICES_PER_THREAD = new ConfigOption<>(
                    "allocator.max_vertices_per_thread",
                    "Maximum number of vertices per thread processed " +
                    "in each memory allocator",
                    positiveInt(),
                    10000
            );

    public static Set<String> REQUIRED_OPTIONS = ImmutableSet.of(
    );

    public static final ConfigOption<String> JOB_ID =
            new ConfigOption<>(
                    "job.id",
                    "The job id on Yarn cluster or K8s cluster.",
                    disallowEmpty(),
                    "local_0001"
            );

    public static final ConfigOption<Integer> JOB_WORKERS_COUNT =
            new ConfigOption<>(
                    "job.workers_count",
                    "The workers count for computing one graph " +
                    "algorithm job.",
                    positiveInt(),
                    1
            );

    public static final ConfigOption<Integer> JOB_PARTITIONS_COUNT =
            new ConfigOption<>(
                    "job.partitions_count",
                    "The partitions count for computing one graph " +
                    "algorithm job.",
                    positiveInt(),
                    1
            );

    public static final ConfigOption<Integer> BSP_MAX_SUPER_STEP =
            new ConfigOption<>(
                    "bsp.max_super_step",
                    "The max super step of the algorithm.",
                    positiveInt(),
                    10
            );

    public static final ConfigOption<String> BSP_ETCD_ENDPOINTS =
            new ConfigOption<>(
                    "bsp.etcd_endpoints",
                    "The end points to access etcd.",
                    disallowEmpty(),
                    "http://localhost:2379"
            );

    public static final ConfigOption<Long> BSP_REGISTER_TIMEOUT =
            new ConfigOption<>(
                    "bsp.register_timeout",
                    "The max timeout to wait for master and works to register.",
                    positiveInt(),
                    TimeUnit.MINUTES.toMillis(5L)
            );

    public static final ConfigOption<Long> BSP_WAIT_WORKERS_TIMEOUT =
            new ConfigOption<>(
                    "bsp.wait_workers_timeout",
                    "The max timeout to wait for workers bsp event.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24L)
            );

    public static final ConfigOption<Long> BSP_WAIT_MASTER_TIMEOUT =
            new ConfigOption<>(
                    "bsp.wait_master_timeout",
                    "The max timeout(in ms) to wait for master bsp event.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24L)
            );

    public static final ConfigOption<Long> BSP_LOG_INTERVAL =
            new ConfigOption<>(
                    "bsp.log_interval",
                    "The log interval(in ms) to print the log while " +
                    "waiting bsp event.",
                    positiveInt(),
                    TimeUnit.SECONDS.toMillis(30L)
            );

    public static final ConfigOption<Class<?>> WORKER_PARTITIONER =
            new ConfigOption<>(
                    "worker.partitioner",
                    "The partitioner that decides which partition a vertex " +
                    "should be in, and which worker a partition should be in.",
                    disallowEmpty(),
                    HashPartitioner.class
            );

    public static final ConfigOption<Class<?>> WORKER_COMPUTATION_CLASS =
            new ConfigOption<>(
                    "worker.computation_class",
                    "The class to create worker-computation object, " +
                    "worker-computation is used to compute each vertex " +
                    "in each superstep.",
                    disallowEmpty(),
                    Null.class
            );

    public static final ConfigOption<Class<?>> WORKER_COMBINER_CLASS =
            new ConfigOption<>(
                    "worker.combiner_class",
                    "Combiner can combine messages into one value for a " +
                    "vertex, for example page-rank algorithm can combine " +
                    "messages of a vertex to a sum value.",
                    disallowEmpty(),
                    Null.class
            );

    public static final ConfigOption<Class<?>>
            WORKER_VERTEX_PROPERTIES_COMBINER_CLASS =
            new ConfigOption<>(
                    "worker.vertex_properties_combiner_class",
                    "The combiner can combine several properties of the same " +
                    "vertex into one properties at inputstep.",
                    disallowEmpty(),
                    OverwriteCombiner.class
            );

    public static final ConfigOption<Class<?>>
            WORKER_EDGE_PROPERTIES_COMBINER_CLASS =
            new ConfigOption<>(
                    "worker.edge_properties_combiner_class",
                    "The combiner can combine several properties of the same " +
                    "edge into one properties at inputstep.",
                    disallowEmpty(),
                    OverwriteCombiner.class
            );

    public static final ConfigOption<Long> WORKER_RECEIVED_BUFFERS_BYTES_LIMIT =
            new ConfigOption<>(
                    "worker.received_buffers_bytes_limit",
                    "The limit bytes of buffers of received data, " +
                    "the total size of all buffers can't excess this limit. " +
                    "If received buffers reach this limit, they will be " +
                    "merged into a file.",
                    positiveInt(),
                    100 * Bytes.MB
            );

    public static final ConfigOption<Long> WORKER_WAIT_SORT_TIMEOUT =
            new ConfigOption<>(
                    "worker.wait_sort_timeout",
                    "The max timeout(in ms) message-handler wait for " +
                    "sort-thread to sort one batch of buffers.",
                    positiveInt(),
                    TimeUnit.MINUTES.toMillis(10)
            );

    public static final ConfigOption<Long> WORKER_WAIT_FINISH_MESSAGES_TIMEOUT =
            new ConfigOption<>(
                    "worker.wait_finish_messages_timeout",
                    "The max timeout(in ms) message-handler wait for " +
                    "finish-message of all workers.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24)
            );

    public static final ConfigListOption<String> WORKER_DATA_DIRS =
            new ConfigListOption<>(
                    "worker.data_dirs",
                    true,
                    "The directories separated by ',' that received " +
                    "vertices and messages can persist into.",
                    disallowEmpty(),
                    String.class,
                    ImmutableList.of("jobs")
            );

    public static final ConfigOption<Integer> WORKER_WRITE_BUFFER_THRESHOLD =
            new ConfigOption<>(
                    "worker.write_buffer_threshold",
                    "The threshold of write buffer, exceeding it will " +
                    "trigger sorting, the write buffer is used to store " +
                    "vertex or message.",
                    positiveInt(),
                    (int) (50 * Bytes.KB)
            );

    public static final ConfigOption<Integer>
            WORKER_WRITE_BUFFER_INIT_CAPACITY = new ConfigOption<>(
                    "worker.write_buffer_capacity",
                    "The initial size of write buffer that used to store " +
                    "vertex or message.",
                    positiveInt(),
                    (int) (60 * Bytes.KB)
            );

    public static final ConfigOption<Class<?>> MASTER_COMPUTATION_CLASS =
            new ConfigOption<>(
                    "master.computation_class",
                    "Master-computation is computation that can determine " +
                    "whether to continue next superstep. It runs at the end " +
                    "of each superstep on master.",
                    disallowEmpty(),
                    DefaultMasterComputation.class
            );

    public static final ConfigOption<String> HUGEGRAPH_URL =
            new ConfigOption<>(
                    "hugegraph.url",
                    "The hugegraph url to load data and write results back.",
                    disallowEmpty(),
                    "http://127.0.0.1:8080"
            );

    public static final ConfigOption<String> HUGEGRAPH_GRAPH_NAME =
            new ConfigOption<>(
                    "hugegraph.name",
                    "The graph name to load data and write results back.",
                    disallowEmpty(),
                    "hugegraph"
            );

    public static final ConfigOption<String> TRANSPORT_SERVER_HOST =
            new ConfigOption<>(
                    "transport.server_host",
                    "The server hostname or ip to listen on to transfer data.",
                    disallowEmpty(),
                    "127.0.0.1"
            );

    public static final ConfigOption<Integer> TRANSPORT_SERVER_PORT =
            new ConfigOption<>(
                    "transport.server_port",
                    "The server port to listen on to transfer data. " +
                    "The system will assign a random port if it's set to 0.",
                    nonNegativeInt(),
                    0
            );

    public static final ConfigOption<Integer> TRANSPORT_SERVER_THREADS =
            new ConfigOption<>(
                    "transport.server_threads",
                    "The number of transport threads for server, the default " +
                    "value is CPUs.",
                    positiveInt(),
                    TransportConf.NUMBER_CPU_CORES
            );

    public static final ConfigOption<Integer> TRANSPORT_CLIENT_THREADS =
            new ConfigOption<>(
                    "transport.client_threads",
                    "The number of transport threads for client, the default " +
                    "value is CPUs.",
                    positiveInt(),
                    TransportConf.NUMBER_CPU_CORES
            );

    public static final ConfigOption<Class<?>> TRANSPORT_PROVIDER_CLASS =
            new ConfigOption<>(
                    "transport.provider_class",
                    "The transport provider, currently only supports Netty.",
                    disallowEmpty(),
                    NettyTransportProvider.class
            );

    public static final ConfigOption<String> TRANSPORT_IO_MODE =
            new ConfigOption<>(
                    "transport.io_mode",
                    "The network IO Mode, either 'NIO', 'EPOLL', 'AUTO', the " +
                    "'AUTO' means selecting the property mode automatically.",
                    allowValues("NIO", "EPOLL", "AUTO"),
                    "AUTO"
            );

    public static final ConfigOption<Boolean> TRANSPORT_EPOLL_LT =
            new ConfigOption<>(
                    "transport.transport_epoll_lt",
                    "Whether enable EPOLL level-trigger.",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> TRANSPORT_TCP_KEEP_ALIVE =
            new ConfigOption<>(
                    "transport.transport_tcp_keep_alive",
                    "Whether enable TCP keep-alive.",
                    allowValues(true, false),
                    true
            );

    public static final ConfigOption<Integer> TRANSPORT_MAX_SYN_BACKLOG =
            new ConfigOption<>(
                    "transport.max_syn_backlog",
                    "The capacity of SYN queue on server side, 0 means using " +
                    "system default value.",
                    nonNegativeInt(),
                    511
            );

    public static final ConfigOption<Integer> TRANSPORT_RECEIVE_BUFFER_SIZE =
            new ConfigOption<>(
                    "transport.receive_buffer_size",
                    "The size of socket receive-buffer in bytes, 0 means " +
                    "using system default value.",
                    nonNegativeInt(),
                    0
            );

    public static final ConfigOption<Integer> TRANSPORT_SEND_BUFFER_SIZE =
            new ConfigOption<>(
                    "transport.send_buffer_size",
                    "The size of socket send-buffer in bytes, 0 means using " +
                    "system default value.",
                    nonNegativeInt(),
                    // TODO: Test to get an best value
                    0
            );

    public static final ConfigOption<Long> TRANSPORT_CLIENT_CONNECT_TIMEOUT =
            new ConfigOption<>(
                    "transport.client_connect_timeout",
                    "The timeout(in ms) of client connect to server.",
                    positiveInt(),
                    3000L
            );

    public static final ConfigOption<Long> TRANSPORT_CLOSE_TIMEOUT =
            new ConfigOption<>(
                    "transport.close_timeout",
                    "The timeout(in ms) of close server or close client.",
                    positiveInt(),
                    10_000L
            );

    public static final ConfigOption<Long> TRANSPORT_SYNC_REQUEST_TIMEOUT =
            new ConfigOption<>(
                    "transport.sync_request_timeout",
                    "The timeout(in ms) to wait response after " +
                    "sending sync-request.",
                    positiveInt(),
                    5_000L
            );

    public static final ConfigOption<Long> TRANSPORT_FINISH_SESSION_TIMEOUT =
            new ConfigOption<>(
                    "transport.finish_session_timeout",
                    "The timeout(in ms) to finish session, " +
                    "0 means using (transport.sync_request_timeout * " +
                    "transport.max_pending_requests).",
                    nonNegativeInt(),
                    0L
            );

    public static final ConfigOption<Long> TRANSPORT_WRITE_SOCKET_TIMEOUT =
            new ConfigOption<>(
                    "transport.write_socket_timeout",
                    "The timeout(in ms) to write data to socket buffer.",
                    positiveInt(),
                    3000L
            );

    public static final ConfigOption<Integer> TRANSPORT_NETWORK_RETRIES =
            new ConfigOption<>(
                    "transport.network_retries",
                    "The number of retry attempts for network communication," +
                    "if network unstable.",
                    nonNegativeInt(),
                    3
            );

    public static final ConfigOption<Integer> TRANSPORT_WRITE_BUFFER_HIGH_MARK =
            new ConfigOption<>(
                    "transport.write_buffer_high_mark",
                    "The high water mark for write buffer in bytes, " +
                    "it will trigger the sending unavailable if the number " +
                    "of queued bytes > write_buffer_high_mark.",
                    nonNegativeInt(),
                    64 * (int) Bytes.MB
            );

    public static final ConfigOption<Integer> TRANSPORT_WRITE_BUFFER_LOW_MARK =
            new ConfigOption<>(
                    "transport.write_buffer_low_mark",
                    "The low water mark for write buffer in bytes, it will " +
                    "trigger the sending available if the number of queued " +
                    "bytes < write_buffer_low_mark." +
                    nonNegativeInt(),
                    32 * (int) Bytes.MB
            );

    public static final ConfigOption<Integer> TRANSPORT_MAX_PENDING_REQUESTS =
            new ConfigOption<>(
                    "transport.max_pending_requests",
                    "The max number of client unreceived ack, " +
                    "it will trigger the sending unavailable if the number " +
                    "of unreceived ack >= max_pending_requests.",
                    positiveInt(),
                    8000
            );

    public static final ConfigOption<Integer> TRANSPORT_MIN_PENDING_REQUESTS =
            new ConfigOption<>(
                    "transport.min_pending_requests",
                    "The minimum number of client unreceived ack, " +
                    "it will trigger the sending available if the number of " +
                    "unreceived ack < min_pending_requests.",
                    positiveInt(),
                    6000
            );

    public static final ConfigOption<Long> TRANSPORT_MIN_ACK_INTERVAL =
            new ConfigOption<>(
                    "transport.min_ack_interval",
                    "The minimum interval(in ms) of server reply ack.",
                    positiveInt(),
                    200L
            );

    public static final ConfigOption<Long> TRANSPORT_SERVER_IDLE_TIMEOUT =
            new ConfigOption<>(
                    "transport.server_idle_timeout",
                    "The max timeout(in ms) of server idle.",
                    positiveInt(),
                    120_000L
            );

    public static final ConfigOption<Long> TRANSPORT_HEARTBEAT_INTERVAL =
            new ConfigOption<>(
                    "transport.heartbeat_interval",
                    "The minimum interval(in ms) between heartbeats on " +
                    "client side.",
                    positiveInt(),
                    20_000L
            );

    public static final ConfigOption<Integer>
            TRANSPORT_MAX_TIMEOUT_HEARTBEAT_COUNT =
            new ConfigOption<>(
                    "transport.max_timeout_heartbeat_count",
                    "The maximum times of timeout heartbeat on client side, " +
                    "if the number of timeouts waiting for heartbeat " +
                    "response continuously > max_heartbeat_timeouts the " +
                    "channel will be closed from client side.",
                    positiveInt(),
                    90
            );

    public static final ConfigOption<Long> HGKV_MAX_FILE_SIZE =
            new ConfigOption<>(
                    "hgkv.max_file_size",
                    "The max number of bytes in each hgkv-file.",
                    positiveInt(),
                    Bytes.GB * 4
            );

    public static final ConfigOption<Long> HGKV_DATABLOCK_SIZE =
            new ConfigOption<>(
                    "hgkv.max_data_block_size",
                    "The max byte size of hgkv-file data block.",
                    positiveInt(),
                    Bytes.KB * 64
            );

    public static final ConfigOption<Integer> HGKV_MERGE_FILES_NUM =
            new ConfigOption<>(
                    "hgkv.max_merge_files",
                    "The max number of files to merge at one time.",
                    positiveInt(),
                    // TODO: test if the default value is appropriate.
                    200
            );

    public static final ConfigOption<String> HGKV_TEMP_DIR =
            new ConfigOption<>(
                    "hgkv.temp_file_dir",
                    "This folder is used to store temporary files, temporary " +
                    "files will be generated during the file merging process.",
                    disallowEmpty(),
                    "/tmp/hgkv"
            );

    public static final ConfigOption<Long> VALUE_FILE_MAX_SEGMENT_SIZE =
            new ConfigOption<>(
                    "valuefile.max_segment_size",
                    "The max number of bytes in each segment of value-file.",
                    positiveInt(),
                    Bytes.GB
            );
}
