/*
 * Copyright (C) 2021 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.hugegraph.computer.core.util;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeByteArrayOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;

public class IdValueUtil {

    // TODO: try to reduce call ComputerContext.instance() directly.
    private static final ComputerContext CONTEXT = ComputerContext.instance();

    public static Id toId(IdValue idValue) {
        byte[] bytes = idValue.bytes();
        try (UnsafeByteArrayInput bai =
             new OptimizedUnsafeByteArrayInput(bytes);
             GraphInput input = new StreamGraphInput(CONTEXT, bai)) {
            return input.readId();
        } catch (IOException e) {
            throw new ComputerException("Failed to get id from idValue '%s'",
                                        e, idValue);
        }
    }

    public static IdValue toIdValue(Id id, int len) {
        try (UnsafeByteArrayOutput bao =
             new OptimizedUnsafeByteArrayOutput(len);
             GraphOutput output = new StreamGraphOutput(CONTEXT, bao)) {
            output.writeId(id);
            return new IdValue(bao.toByteArray());
        } catch (IOException e) {
            throw new ComputerException("Failed to get idValue from id '%s'",
                                        e, id);
        }
    }
}
