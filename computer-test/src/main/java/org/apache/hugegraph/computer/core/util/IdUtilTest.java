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

package org.apache.hugegraph.computer.core.util;

import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class IdUtilTest {

    @Test
    public void testParseId() {
        String idUtf8WithString = "\"abc\"";
        String idUtf8WithNumber = "\"222\"";
        String idLong = "222";
        String idUuid = "U\"3b676b77-c484-4ba6-b627-8c040bc42863\"";

        String idNull = null;
        String idEmpty = "";
        String idDouble = "1.23";
        String idUuidInvalid = "U\"123\"";

        Assert.assertEquals(IdType.UTF8, IdUtil.parseId(idUtf8WithString).idType());
        Assert.assertEquals(IdType.UTF8, IdUtil.parseId(idUtf8WithNumber).idType());
        Assert.assertEquals(IdType.LONG, IdUtil.parseId(idLong).idType());
        Assert.assertEquals(IdType.UUID, IdUtil.parseId(idUuid).idType());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IdUtil.parseId(idNull).idType();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IdUtil.parseId(idEmpty).idType();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IdUtil.parseId(idDouble).idType();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IdUtil.parseId(idUuidInvalid).idType();
        });
    }
}
