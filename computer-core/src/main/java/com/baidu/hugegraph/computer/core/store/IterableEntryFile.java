package com.baidu.hugegraph.computer.core.store;

import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;

public interface IterableEntryFile {

    EntryIterator iterator();
}
