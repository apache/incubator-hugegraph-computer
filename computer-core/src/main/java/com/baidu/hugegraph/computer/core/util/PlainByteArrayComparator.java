/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.core.util;


import com.google.common.base.Preconditions;

public class PlainByteArrayComparator {

    public static int compare(byte[] o1, byte[] o2) {
        Preconditions.checkNotNull(o1);
        Preconditions.checkNotNull(o2);
        return compare(o1, 0, o1.length, o2, 0, o2.length);
    }

    public static int compare(byte[] o1, int length1, byte[] o2, int length2) {
        Preconditions.checkNotNull(o1);
        Preconditions.checkNotNull(o2);
        return compare(o1, 0, length1, o2, 0, length2);
    }

    public static int compare(byte[] buffer1, int offset1, int length1,
                              byte[] buffer2, int offset2, int length2) {
        if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
            return 0;
        }
        if (length1 != length2) {
            return length1 - length2;
        }

        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return 0;
    }
}
