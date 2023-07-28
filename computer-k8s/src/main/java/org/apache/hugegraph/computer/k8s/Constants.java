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

package org.apache.hugegraph.computer.k8s;

import java.util.List;

import com.google.common.collect.Lists;

import io.fabric8.kubernetes.client.utils.URLUtils;

public class Constants {

    public static final String ALL_NAMESPACE = "*";
    public static final String DEFAULT_NAMESPACE = "hugegraph-computer-operator-system";

    public static final int MASTER_INSTANCES = 1;

    // Container ENV
    public static final String ENV_POD_IP = "POD_IP";
    public static final String ENV_POD_NAMESPACE = "POD_NAMESPACE";
    public static final String ENV_POD_NAME = "POD_NAME";
    public static final String ENV_CONFIG_DIR = "CONFIG_DIR";
    public static final String ENV_COMPUTER_CONF_PATH = "COMPUTER_CONF_PATH";
    public static final String ENV_LOG4J_CONF_PATH = "LOG4J_CONF_PATH";
    public static final String ENV_JAR_FILE_PATH = "JAR_FILE_PATH";
    public static final String ENV_REMOTE_JAR_URI = "REMOTE_JAR_URI";
    public static final String ENV_JVM_OPTIONS = "JVM_OPTIONS";
    public static final String ENV_CPU_LIMIT = "CPU_LIMIT";
    public static final String ENV_MEMORY_LIMIT = "MEMORY_LIMIT";

    public static final String CONFIG_DIR =  "/opt/hugegraph-computer/conf";
    public static final String COMPUTER_CONF_FILE = "computer.properties";
    public static final String LOG_XML_FILE = "log4j2.xml";
    public static final String COMPUTER_CONF_PATH =
           URLUtils.pathJoin(CONFIG_DIR, COMPUTER_CONF_FILE);
    public static final String LOG_XML_PATH =
           URLUtils.pathJoin(CONFIG_DIR, LOG_XML_FILE);

    public static final String DEFAULT_LOG_PATH =
            "/opt/hugegraph-computer/logs/hugegraph-computer.log";

    public static final List<String> COMMAND =
           Lists.newArrayList("bin/start-computer.sh");
    public static final List<String> MASTER_ARGS =
           Lists.newArrayList("-r master", "-d k8s");
    public static final List<String> WORKER_ARGS =
           Lists.newArrayList("-r worker", "-d k8s");

    public static final String K8S_SPEC_PREFIX  = "k8s.";
}
