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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class StringEncodeUtil {

    private static final byte[] BYTES_EMPTY = new byte[0];

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    public static byte[] encode(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String decode(byte[] bytes, int offset, int length) {
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }

    public static String encodeBase64(byte[] bytes) {
        return BASE64_ENCODER.encodeToString(bytes);
    }

    public static byte[] decodeBase64(String value) {
        if (value.isEmpty()) {
            return BYTES_EMPTY;
        }
        return BASE64_DECODER.decode(value);
    }
}
