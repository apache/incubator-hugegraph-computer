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

import com.google.common.base.Preconditions;

public class BytesUtil {

    public static byte[] ensureCapacityWithoutCopy(byte[] bytes, int length) {
        if (bytes == null || bytes.length < length) {
            return new byte[length];
        } else {
            return bytes;
        }
    }

    public static int hashBytes(byte[] bytes, int length) {
        return hashBytes(bytes, 0, length);
    }

    public static int hashBytes(byte[] bytes, int offset, int length) {
        int hash = 1;
        for (int i = offset; i < offset + length; i++) {
            hash = (31 * hash) + (int) bytes[i];
        }
        return hash;
    }

    public static int compare(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes1.length, bytes2, bytes2.length);
    }

    public static int compare(byte[] bytes1, int length1,
                              byte[] bytes2, int length2) {
        return compare(bytes1, 0, length1, bytes2, 0, length2);
    }

    // TODO: use google comparator(unsafe) to improve perf
    public static int compare(byte[] bytes1, int offset1, int length1,
                              byte[] bytes2, int offset2, int length2) {
        Preconditions.checkNotNull(bytes1);
        Preconditions.checkNotNull(bytes2);
        if (bytes1 == bytes2 && offset1 == offset2 && length1 == length2) {
            return 0;
        }
        if (length1 != length2) {
            return length1 - length2;
        }

        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (bytes1[i] & 0xff);
            int b = (bytes2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return 0;
    }
}
