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

package org.apache.hugegraph.computer.core.input;

import org.apache.hugegraph.computer.core.input.loader.FileInputSplit;
import org.apache.hugegraph.loader.constant.ElemType;
import org.apache.hugegraph.loader.mapping.InputStruct;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class FileInputSplitTest {

    @Test
    public void testConstructor() {
        InputStruct inputStruct = Mockito.mock(InputStruct.class);
        FileInputSplit split = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                  "/tmp/test");
        Assert.assertEquals("/tmp/test", split.path());
        Assert.assertEquals(inputStruct, split.struct());
        Assert.assertSame(ElemType.VERTEX, split.type());
    }

    @Test
    public void testEquals() {
        InputStruct inputStruct = Mockito.mock(InputStruct.class);
        FileInputSplit split1 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        FileInputSplit split2 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        Assert.assertEquals(split1, split1);
        Assert.assertEquals(split1, split2);

        Assert.assertNotEquals(split1, null);
        Assert.assertNotEquals(split1, new Object());

        Assert.assertEquals(InputSplit.END_SPLIT, InputSplit.END_SPLIT);
        Assert.assertNotEquals(InputSplit.END_SPLIT, split1);
    }

    @Test
    public void testHashCode() {
        InputStruct inputStruct = Mockito.mock(InputStruct.class);
        FileInputSplit split1 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        FileInputSplit split2 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        Assert.assertEquals(split1.hashCode(), split2.hashCode());
    }
}
