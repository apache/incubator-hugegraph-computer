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

package com.baidu.hugegraph.computer.core.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;

public class HdfsUtil {

    public static FileSystem openHDFS(Config config, Configuration conf)
                                      throws IOException, URISyntaxException,
                                             InterruptedException {
        String url = config.get(ComputerOptions.HDFS_URL);
        Boolean enableKerberos = config.get(
                                 ComputerOptions.HDFS_KERBEROS_ENABLE);

        if (enableKerberos) {
            String krb5Conf = config.get(ComputerOptions.HDFS_KRB5_CONF);
            System.setProperty("java.security.krb5.conf", krb5Conf);
            String principal = config.get(
                               ComputerOptions.HDFS_KERBEROS_PRINCIPAL);
            String keyTab = config.get(ComputerOptions.HDFS_KERBEROS_KEYTAB);
            conf.set("fs.defaultFS", url);
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("dfs.namenode.kerberos.principal", principal);

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            return FileSystem.get(conf);
        } else {
            String user = config.get(ComputerOptions.HDFS_USER);
            return FileSystem.get(URI.create(url), conf, user);
        }
    }
}
