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

package org.apache.hugegraph.computer.core.combiner;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.StreamGraphInput;
import org.apache.hugegraph.computer.core.store.entry.InlinePointer;
import org.apache.hugegraph.computer.core.store.entry.Pointer;

public class VertexValueCombiner extends AbstractPointerCombiner<Properties> {

    public VertexValueCombiner(ComputerContext context) {
        super(() -> {
            return context.graphFactory().createProperties();
        }, context.config().createObject(
           ComputerOptions.WORKER_VERTEX_PROPERTIES_COMBINER_CLASS));
    }

    @Override
    public Pointer combine(Pointer v1, Pointer v2) {
        try {
            RandomAccessInput input1 = v1.input();
            RandomAccessInput input2 = v2.input();
            input1.seek(v1.offset());
            input2.seek(v2.offset());
            String label1 = StreamGraphInput.readLabel(input1);
            String label2 = StreamGraphInput.readLabel(input2);
            assert label1.equals(label2);

            this.v1.read(input1);
            this.v2.read(input2);
            this.combiner.combine(this.v1, this.v2, this.result);

            this.output.seek(0L);
            this.output.writeUTF(label1);
            this.result.write(this.output);
            return new InlinePointer(this.output.buffer(),
                                     this.output.position());
        } catch (Exception e) {
            throw new ComputerException(
                    "Failed to combine pointer1(offset=%s, length=%s) and " +
                    "pointer2(offset=%s, length=%s)'",
                    e, v1.offset(), v1.length(), v2.offset(), v2.length());
        }
    }
}
