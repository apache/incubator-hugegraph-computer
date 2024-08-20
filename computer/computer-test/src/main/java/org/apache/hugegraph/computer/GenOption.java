/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hugegraph.computer;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.hugegraph.computer.k8s.config.KubeDriverOptions;
import org.apache.hugegraph.config.OptionHolder;
import org.apache.hugegraph.config.TypedOption;

public class GenOption {

    public static void main(String[] args) {
        OptionHolder instance = KubeDriverOptions.instance();

        Map<String, TypedOption<?, ?>> options = instance.options();

        StrBuilder strBuilder = new StrBuilder();

        options.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(
                Collectors.toList()).forEach(enrtry -> {
            TypedOption<?, ?> value = enrtry.getValue();
            Object o = value.defaultValue();
            String defaultV = o instanceof Class ? ((Class<?>) o).getCanonicalName() :
                              String.valueOf(o);
            strBuilder.append("|");
            strBuilder.append(value.name()).append("|").append(defaultV).append("|")
                      .append(value.desc()).append("|").append("\n");
        });

        System.out.println(strBuilder);
    }
}
