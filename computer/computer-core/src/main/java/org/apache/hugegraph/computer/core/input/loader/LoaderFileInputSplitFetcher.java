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

package org.apache.hugegraph.computer.core.input.loader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.computer.core.input.InputSplitFetcher;
import org.apache.hugegraph.loader.constant.ElemType;
import org.apache.hugegraph.loader.exception.LoadException;
import org.apache.hugegraph.loader.mapping.InputStruct;
import org.apache.hugegraph.loader.mapping.LoadMapping;
import org.apache.hugegraph.loader.source.SourceType;
import org.apache.hugegraph.loader.source.file.FileFilter;
import org.apache.hugegraph.loader.source.file.FileSource;
import org.apache.hugegraph.loader.source.hdfs.HDFSSource;
import org.apache.hugegraph.loader.source.hdfs.KerberosConfig;

public class LoaderFileInputSplitFetcher implements InputSplitFetcher {

    private final Config config;
    private final List<InputStruct> vertexInputStructs;
    private final List<InputStruct> edgeInputStructs;

    public LoaderFileInputSplitFetcher(Config config) {
        this.config = config;
        String inputStructFile = this.config.get(
                                      ComputerOptions.INPUT_LOADER_STRUCT_PATH);
        LoadMapping mapping = LoadMapping.of(inputStructFile);
        this.vertexInputStructs = new ArrayList<>();
        this.edgeInputStructs = new ArrayList<>();
        this.splitStructs(mapping.structs());
    }

    @Override
    public List<InputSplit> fetchVertexInputSplits() {
        List<InputSplit> splits = new ArrayList<>();
        for (InputStruct vertexInputStruct : this.vertexInputStructs) {
            FileSource source = (FileSource) vertexInputStruct.input();
            List<String> paths = this.scanPaths(source);
            if (CollectionUtils.isNotEmpty(paths)) {
                for (String path : paths) {
                    FileInputSplit split = new FileInputSplit(ElemType.VERTEX,
                                                              vertexInputStruct,
                                                              path);
                    splits.add(split);
                }
            }
        }
        return splits;
    }

    @Override
    public List<InputSplit> fetchEdgeInputSplits() {
        List<InputSplit> splits = new ArrayList<>();
        for (InputStruct edgeInputStruct : this.edgeInputStructs) {
            FileSource source = (FileSource) edgeInputStruct.input();
            List<String> paths = this.scanPaths(source);
            if (CollectionUtils.isNotEmpty(paths)) {
                for (String path : paths) {
                    FileInputSplit split = new FileInputSplit(ElemType.EDGE,
                                                              edgeInputStruct,
                                                              path);
                    splits.add(split);
                }
            }
        }
        return splits;
    }

    private void splitStructs(List<InputStruct> structs) {
        for (InputStruct struct : structs) {
            InputStruct result = struct.extractVertexStruct();
            if (result != InputStruct.EMPTY) {
                this.vertexInputStructs.add(result);
            }
        }
        for (InputStruct struct : structs) {
            InputStruct result = struct.extractEdgeStruct();
            if (result != InputStruct.EMPTY) {
                this.edgeInputStructs.add(result);
            }
        }
    }

    private List<String> scanPaths(FileSource source) {
        if (source.type() == SourceType.HDFS) {
            return this.scanHdfsPaths((HDFSSource) source);
        } else {
            return this.scanLocalPaths(source);
        }
    }

    private List<String> scanLocalPaths(FileSource source) {
        List<String> paths = new ArrayList<>();
        File file = FileUtils.getFile(source.path());
        FileFilter filter = source.filter();
        if (file.isFile()) {
            if (!filter.reserved(file.getName())) {
                throw new LoadException(
                      "Please check file name and extensions, ensure " +
                      "that at least one file is available for reading");
            }
            paths.add(file.getAbsolutePath());
        } else {
            assert file.isDirectory();
            File[] subFiles = file.listFiles();
            if (subFiles == null) {
                throw new LoadException("Error while listing the files of " +
                                        "path '%s'", file);
            }
            for (File subFile : subFiles) {
                if (filter.reserved(subFile.getName())) {
                    paths.add(subFile.getAbsolutePath());
                }
            }
        }
        return paths;
    }

    private List<String> scanHdfsPaths(HDFSSource hdfsSource) {
        List<String> paths = new ArrayList<>();
        try {
            Configuration configuration = this.loadConfiguration(hdfsSource);
            this.enableKerberos(hdfsSource, configuration);
            try (FileSystem hdfs = FileSystem.get(configuration)) {
                Path path = new Path(hdfsSource.path());
                FileFilter filter = hdfsSource.filter();
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

    @Override
    public void close() {
        // pass
    }
}
