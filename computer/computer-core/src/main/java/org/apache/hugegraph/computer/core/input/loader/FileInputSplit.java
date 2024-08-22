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


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.loader.constant.ElemType;
import org.apache.hugegraph.loader.mapping.InputStruct;

import com.google.common.base.Objects;

public class FileInputSplit extends InputSplit {

    private final ElemType type;
    private final String path;
    private final InputStruct struct;

    public FileInputSplit(ElemType type, InputStruct struct, String path) {
        super(StringUtils.EMPTY, StringUtils.EMPTY);
        this.type = type;
        this.struct = struct;
        this.path = path;
    }

    public ElemType type() {
        return this.type;
    }

    public String path() {
        return this.path;
    }

    public InputStruct struct() {
        return this.struct;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FileInputSplit other = (FileInputSplit) o;
        return this.type == other.type &&
               Objects.equal(this.path, other.path) &&
               Objects.equal(this.struct, other.struct);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), this.type,
                                this.path, this.struct);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("type", this.type)
                                        .append("path", this.path)
                                        .append("struct", this.struct)
                                        .toString();
    }
}
