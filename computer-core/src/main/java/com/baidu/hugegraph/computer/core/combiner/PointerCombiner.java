package com.baidu.hugegraph.computer.core.combiner;

import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;

public interface PointerCombiner {

    Pointer combine(Pointer v1, Pointer v2);
}
