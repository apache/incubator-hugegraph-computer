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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.Writable;

public final class SerializeUtil {

    public static byte[] toBytes(Writable obj) {
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            obj.write(bao);
            return bao.toByteArray();
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to create byte array with writable '%s'", e, obj);
        }
    }

    public static byte[] toBytes(List<? extends Writable> list) {
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            bao.writeInt(list.size());
            for (Writable obj : list) {
                obj.write(bao);
            }
            return bao.toByteArray();
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to create byte array with List<Writable> '%s'",
                      e, list);
        }
    }

    public static void fromBytes(byte[] bytes, Readable obj) {
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            obj.read(bai);
        } catch (IOException e) {
            throw new ComputerException("Failed to read from byte array", e);
        }
    }

    public static <V extends Readable> List<V> fromBytes(byte[] bytes,
                                                         Supplier<V> supplier) {
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            int size = bai.readInt();
            List<V> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                V obj = supplier.get();
                obj.read(bai);
                list.add(obj);
            }
            return list;
        } catch (IOException e) {
            throw new ComputerException("Failed to read from byte array", e);
        }
    }
}
