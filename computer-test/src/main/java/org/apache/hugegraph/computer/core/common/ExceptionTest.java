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

package org.apache.hugegraph.computer.core.common;

import java.io.IOException;
import java.nio.charset.IllegalCharsetNameException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class ExceptionTest {

    @Test
    public void testComputerException() {
        Assert.assertThrows(ComputerException.class, () -> {
            throw new ComputerException("computer exception");
        }, e -> {
            Assert.assertEquals("computer exception", e.getMessage());
            Assert.assertNull(e.getCause());
        });

        Assert.assertThrows(ComputerException.class, () -> {
            throw new ComputerException("computer exception",
                                        new IOException());
        }, e -> {
            Assert.assertEquals("computer exception", e.getMessage());
            Assert.assertEquals(IOException.class, e.getCause().getClass());
        });

        Assert.assertThrows(ComputerException.class, () -> {
            throw new ComputerException("computer exception at step %s", 1);
        }, e -> {
            Assert.assertEquals("computer exception at step 1",
                                e.getMessage());
            Assert.assertNull(e.getCause());
        });

        Assert.assertThrows(ComputerException.class, () -> {
            throw new ComputerException("computer exception at step %s",
                                        new IOException(), 1);
        }, e -> {
            Assert.assertEquals("computer exception at step 1",
                                e.getMessage());
            Assert.assertEquals(IOException.class, e.getCause().getClass());
        });


        Throwable rootCause = new IllegalCharsetNameException("invalid");
        Assert.assertThrows(ComputerException.class, () -> {
            throw new ComputerException("computer exception",
                                        new IOException(rootCause));
        }, e -> {
            Assert.assertEquals("computer exception", e.getMessage());
            Assert.assertEquals(IOException.class, e.getCause().getClass());
            Assert.assertEquals(rootCause, ComputerException.rootCause(e));
        });
    }
}
