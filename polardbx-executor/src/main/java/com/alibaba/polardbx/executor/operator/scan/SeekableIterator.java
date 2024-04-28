package com.alibaba.polardbx.executor.operator.scan;

import java.util.Iterator;

public interface SeekableIterator<E> extends Iterator<E> {
    E seek(int position);
}
