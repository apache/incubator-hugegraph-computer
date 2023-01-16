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

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class InputSplitTest {

    @Test
    public void testConstructor() {
        InputSplit split = new InputSplit("0", "100");
        Assert.assertEquals("0", split.start());
        Assert.assertEquals("100", split.end());
    }

    @Test
    public void testEquals() {
        InputSplit split1 = new InputSplit("0", "100");
        InputSplit split2 = new InputSplit("0", "100");
        Assert.assertEquals(split1, split1);
        Assert.assertEquals(split1, split2);

        Assert.assertNotEquals(split1, null);
        Assert.assertNotEquals(split1, new Object());

        Assert.assertEquals(InputSplit.END_SPLIT, InputSplit.END_SPLIT);
        Assert.assertNotEquals(InputSplit.END_SPLIT, split1);
    }

    @Test
    public void testHashCode() {
        InputSplit split = new InputSplit("0", "100");
        Assert.assertEquals(51074, split.hashCode());
    }
}
