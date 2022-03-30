package com.alibaba.polardbx.common.utils.hash;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
/**
 * 实现与DN端对齐
 */
public abstract class Hasher {

    final int seed;

    HashResult128 result = new HashResult128();

    protected Hasher(int seed) {
        this.seed = seed;
    }

    public HashResult128 getHashResult() {
        return this.result;
    }
}
