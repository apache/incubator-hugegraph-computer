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

package com.baidu.hugegraph.computer.core.input.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.loader.source.file.FileFilter;
import com.baidu.hugegraph.loader.source.file.FileSource;
import com.baidu.hugegraph.loader.source.hdfs.HDFSSource;
import com.baidu.hugegraph.loader.source.hdfs.KerberosConfig;

public class HdfsInputSplitFetcher extends FileInputSplitFetcher {

    public HdfsInputSplitFetcher(Config config) {
        super(config);
    }

    @Override
    protected List<String> scanPaths(FileSource source) {
        List<String> paths = new ArrayList<>();
        try {
            HDFSSource hdfsSource = (HDFSSource) source;
            Configuration configuration = this.loadConfiguration(hdfsSource);
            this.enableKerberos(hdfsSource, configuration);
            try (FileSystem hdfs = FileSystem.get(configuration)) {
                Path path = new Path(source.path());
                FileFilter filter = source.filter();
                if (hdfs.getFileStatus(path).isFile()) {
                    if (!filter.reserved(path.getName())) {
                        throw new ComputerException(
                        "Please check path name and extensions, ensure " +
                        "that at least one path is available for reading");
                    }
                    paths.add(path.toString());
                } else {
                    assert hdfs.getFileStatus(path).isDirectory();
                    FileStatus[] statuses = hdfs.listStatus(path);
                    Path[] subPaths = FileUtil.stat2Paths(statuses);
                    for (Path subPath : subPaths) {
                        if (filter.reserved(subPath.getName())) {
                            paths.add(subPath.toString());
                        }
                    }
                }
            }
        } catch (Throwable throwable) {
            throw new ComputerException("Failed to scanPaths", throwable);
        }

        return paths;
    }

    private Configuration loadConfiguration(HDFSSource source) {
        Configuration conf = new Configuration();
        conf.addResource(new Path(source.coreSitePath()));
        if (source.hdfsSitePath() != null) {
            conf.addResource(new Path(source.hdfsSitePath()));
        }
        return conf;
    }

    private void enableKerberos(HDFSSource source,
                                Configuration conf) throws IOException {
        KerberosConfig kerberosConfig = source.kerberosConfig();
        if (kerberosConfig != null && kerberosConfig.enable()) {
            System.setProperty("java.security.krb5.conf",
                               kerberosConfig.krb5Conf());
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(kerberosConfig.principal(),
                                                     kerberosConfig.keyTab());
        }
    }
}
