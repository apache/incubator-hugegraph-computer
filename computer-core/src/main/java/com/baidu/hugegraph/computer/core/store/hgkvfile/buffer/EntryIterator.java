package com.baidu.hugegraph.computer.core.store.hgkvfile.buffer;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.iterator.CIter;

public interface EntryIterator extends CIter<KvEntry> {

    @Override
    default Object metadata(String meta, Object... args) {
        throw new NotImplementedException();
    }
}
