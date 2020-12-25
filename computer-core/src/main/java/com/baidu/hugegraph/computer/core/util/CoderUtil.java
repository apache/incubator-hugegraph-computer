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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

public class CoderUtil {

    private static final ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
            ThreadLocal.withInitial(() -> Charset.forName("UTF-8").newEncoder()
                       .onMalformedInput(CodingErrorAction.REPORT)
                       .onUnmappableCharacter(CodingErrorAction.REPORT));

    private static final ThreadLocal<CharsetDecoder> DECODER_FACTORY =
            ThreadLocal.withInitial(() -> Charset.forName("UTF-8").newDecoder()
                       .onMalformedInput(CodingErrorAction.REPORT)
                       .onUnmappableCharacter(CodingErrorAction.REPORT));

    public static ByteBuffer encode(String str)
           throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        return encoder.encode(CharBuffer.wrap(str.toCharArray()));
    }

    public static String decode(byte[] utf8, int start, int length)
           throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8, start, length));
    }

    public static String decode(ByteBuffer utf8)
           throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        String str = decoder.decode(utf8).toString();
        return str;
    }
}
