package com.alibaba.polardbx.common.utils.bloomfilter;

import com.alibaba.polardbx.common.utils.hash.BaseHashMethod;
import com.alibaba.polardbx.common.utils.hash.HashMethod;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;

public abstract class BloomFilterHashMethod extends BaseHashMethod implements HashMethod {
    /**
     * 由于udf没有传递seed参数
     * 此处默认seed值需保持与DN一致
     */
    protected static final int DEFAULT_HASH_SEED = 0;

    protected BloomFilterHashMethod(String funcName) {
        super(funcName);
    }

    public abstract IStreamingHasher newHasher();

    public abstract IStreamingHasher getHasher();

    /**
     * BloomFilter hash算法支持64位/128位
     */
    protected abstract boolean is64Bit();
}
