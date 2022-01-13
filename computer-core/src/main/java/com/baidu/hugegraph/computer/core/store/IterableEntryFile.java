package com.baidu.hugegraph.computer.core.store;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;

public interface IterableEntryFile {

    EntryIterator iterator() throws IOException;
}
