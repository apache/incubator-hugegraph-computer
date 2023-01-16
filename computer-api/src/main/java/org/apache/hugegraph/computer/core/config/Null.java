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

package org.apache.hugegraph.computer.core.config;

import org.apache.hugegraph.config.ConfigOption;

/**
 * Null is used in ConfigOption<Class> to indicate a null class option.
 * The config does not allow the default value null, so this is used as the
 * default class value when class option can be null.
 * {@link Config#createObject(ConfigOption)} will return null, if the value
 * is ${@code Null.class}".
 */
public class Null {
}
