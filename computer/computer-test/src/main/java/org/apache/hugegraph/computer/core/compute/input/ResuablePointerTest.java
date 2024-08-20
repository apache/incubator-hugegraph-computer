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

package org.apache.hugegraph.computer.core.compute.input;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class ResuablePointerTest {

    @Test
    public void testReadWrite() throws IOException {
        BytesOutput output1 = IOFactory.createBytesOutput(100);
        long position = output1.position();
        output1.writeFixedInt(0);
        output1.writeInt(Integer.MAX_VALUE);
        int length = (int) (output1.position() - position - Constants.INT_LEN);
        output1.writeFixedInt(position, length);

        ReusablePointer p1 = new ReusablePointer();
        p1.read(IOFactory.createBytesInput(output1.buffer()));

        BytesOutput output2 = IOFactory.createBytesOutput(100);
        p1.write(output2);

        ReusablePointer p2 = new ReusablePointer();
        p2.read(IOFactory.createBytesInput(output2.buffer()));
        Assert.assertEquals(0, p1.compareTo(p2));
        Assert.assertEquals(p1.length(), p2.length());
        Assert.assertEquals(0L, p1.offset());
        Assert.assertEquals(0L, p2.offset());
    }
}
