package com.alibaba.polardbx.executor.gms;

/**
 * Multi-version data or meta-data which could be purged
 */
public interface Purgeable {
    void purge(long tso);
}
