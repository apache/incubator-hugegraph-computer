package com.baidu.hugegraph.computer.core.common;

import java.io.DataInput;
import java.io.IOException;

public interface Readable {

    void read(DataInput in) throws IOException;
}
