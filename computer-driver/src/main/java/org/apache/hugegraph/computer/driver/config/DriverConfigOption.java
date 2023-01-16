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

package org.apache.hugegraph.computer.driver.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.config.ConfigOption;

import com.google.common.base.Predicate;

/**
 * The class is subclass of {@link ConfigOption} it without default value
 */
public class DriverConfigOption<T> extends ConfigOption<T> {

    public DriverConfigOption(String name, Predicate<T> pred,
                              Class<T> type) {
        this(name, StringUtils.EMPTY, pred, type);
    }

    public DriverConfigOption(String name, String desc,
                              Predicate<T> pred, Class<T> type) {
        this(name, false, desc, pred, type);
    }

    public DriverConfigOption(String name, boolean required,
                              String desc, Predicate<T> pred,
                              Class<T> type) {
        super(name, required, desc, pred, type, null);
    }

    public void checkVal(String value) {
        this.parseConvert(value);
    }

    @Override
    protected void check(Object value) {
        if (value == null) {
            return;
        }
        super.check(value);
    }

    @Override
    protected T parse(String value) {
        if (value == null) {
            return null;
        }
        return super.parse(value);
    }

    @Override
    public T parseConvert(String value) {
        if (value == null) {
            return null;
        }
        return super.parseConvert(value);
    }
}
