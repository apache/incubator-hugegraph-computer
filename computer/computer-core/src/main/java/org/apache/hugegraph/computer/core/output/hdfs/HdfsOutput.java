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

package org.apache.hugegraph.computer.core.output.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.output.AbstractComputerOutput;
import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class HdfsOutput extends AbstractComputerOutput {

    private static final Logger LOG = Log.logger(HdfsOutput.class);

    private FileSystem fs;
    private FSDataOutputStream fileOutputStream;
    private String delimiter;
    private static final String REPLICATION_KEY = "dfs.replication";
    private static final String FILE_PREFIX = "partition_";
    private static final String FILE_SUFFIX = ".csv";

    @Override
    public void init(Config config, int partition) {
        super.init(config, partition);

        try {
            Configuration configuration = new Configuration();
            Short replication = config.get(
                                ComputerOptions.OUTPUT_HDFS_REPLICATION);
            configuration.set(REPLICATION_KEY, String.valueOf(replication));
            this.fs = HdfsOutput.openHDFS(config, configuration);

            this.delimiter = config.get(ComputerOptions.OUTPUT_HDFS_DELIMITER);
            String dir = config.get(ComputerOptions.OUTPUT_HDFS_DIR);
            String jobId = config.get(ComputerOptions.JOB_ID);
            Path hdfsPath = buildPath(dir, jobId, partition);
            this.fileOutputStream = this.fs.create(hdfsPath, true);
        } catch (IOException | InterruptedException e) {
            throw new ComputerException("Failed to init hdfs output on " +
                                        "partition [%s]", e, partition);
        }
    }

    @Override
    public void write(Vertex vertex) {
        try {
            if (!this.filter(vertex)) {
                return;
            }
            this.writeString(vertex.id().toString());
            this.writeString(this.delimiter);
            this.writeString(this.constructValueString(vertex));
            this.writeString(System.lineSeparator());
        } catch (IOException e) {
            throw new ComputerException("Failed to write vertex: {}",
                                        vertex.toString(), e);
        }
    }

    protected boolean filter(Vertex vertex) {
        return true;
    }

    protected void writeBytes(byte[] bytes) throws IOException {
        this.fileOutputStream.write(bytes);
    }

    protected void writeString(String string) throws IOException {
        this.writeBytes(StringEncodeUtil.encode(string));
    }

    protected String constructValueString(Vertex vertex) {
        return vertex.value().string();
    }

    public static Path buildPath(String dir, String jobId, int partition) {
        Path dirPath = new Path(dir, jobId);
        return new Path(dirPath, FILE_PREFIX + partition + FILE_SUFFIX);
    }

    @Override
    public void mergePartitions(Config config) {
        Boolean merge = config.get(ComputerOptions.OUTPUT_HDFS_MERGE);
        if (merge) {
            LOG.info("Merge hdfs output partitions started");
            HdfsOutputMerger hdfsOutputMerger = new HdfsOutputMerger();
            try {
                hdfsOutputMerger.init(config);
                hdfsOutputMerger.merge();
            } finally {
                hdfsOutputMerger.close();
            }
            LOG.info("Merge hdfs output partitions finished");
        }
    }

    @Override
    public void close() {
        try {
            if (this.fileOutputStream != null) {
                this.fileOutputStream.close();
            }
            if (this.fs != null) {
                this.fs.close();
            }
        } catch (IOException e) {
            throw new ComputerException("Failed to close hdfs", e);
        }
    }

    public static FileSystem openHDFS(Config config, Configuration conf)
                                      throws IOException,
                                             InterruptedException {
        String url = config.get(ComputerOptions.OUTPUT_HDFS_URL);
        Boolean enableKerberos = config.get(
                ComputerOptions.OUTPUT_HDFS_KERBEROS_ENABLE);

        String coreSite = config.get(
                          ComputerOptions.OUTPUT_HDFS_CORE_SITE_PATH);
        if (StringUtils.isNotBlank(coreSite)) {
            conf.addResource(new Path(coreSite));
        }
        String hdfsSite = config.get(ComputerOptions.OUTPUT_HDFS_SITE_PATH);
        if (StringUtils.isNotBlank(hdfsSite)) {
            conf.addResource(new Path(hdfsSite));
        }

        if (enableKerberos) {
            String krb5Conf = config.get(ComputerOptions.OUTPUT_HDFS_KRB5_CONF);
            System.setProperty("java.security.krb5.conf", krb5Conf);
            String principal = config.get(
                    ComputerOptions.OUTPUT_HDFS_KERBEROS_PRINCIPAL);
            String keyTab = config.get(
                            ComputerOptions.OUTPUT_HDFS_KERBEROS_KEYTAB);
            conf.set("fs.defaultFS", url);
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("dfs.namenode.kerberos.principal", principal);

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            return FileSystem.get(conf);
        } else {
            String user = config.get(ComputerOptions.OUTPUT_HDFS_USER);
            return FileSystem.get(URI.create(url), conf, user);
        }
    }
}
