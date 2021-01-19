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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;

public class CoderUtil {

    private static final ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
            ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder()
                       .onMalformedInput(CodingErrorAction.REPORT)
                       .onUnmappableCharacter(CodingErrorAction.REPORT));

    private static final ThreadLocal<CharsetDecoder> DECODER_FACTORY =
            ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newDecoder()
                       .onMalformedInput(CodingErrorAction.REPORT)
                       .onUnmappableCharacter(CodingErrorAction.REPORT));

    public static ByteBuffer encode(String str) {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        try {
            return encoder.encode(CharBuffer.wrap(str.toCharArray()));
        } catch (CharacterCodingException e) {
            throw new ComputerException("Can't encode %s with UTF-8", e, str);
        }
    }

    public static String decode(byte[] utf8) {
        return decode(utf8, 0, utf8.length);
    }

    public static String decode(byte[] utf8, int start, int length) {
        try {
            return decode(ByteBuffer.wrap(utf8, start, length));
        } catch (CharacterCodingException e) {
            throw new ComputerException("Can't decode bytes, start=%d, " +
                                        "length=%d with UTF-8",
                                        e, start, length);
        }
    }

    public static String decode(ByteBuffer utf8)
                                throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        return decoder.decode(utf8).toString();
    }
}
