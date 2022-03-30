package com.alibaba.polardbx.common.utils.hash;

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterHashMethod;

public class XxHash_64Method extends BloomFilterHashMethod {

    public static final String METHOD_NAME = "xxhash_64";

    private final IStreamingHasher xxHash64Hasher = new XxHash_64Hasher(DEFAULT_HASH_SEED);

    private XxHash_64Method() {
        super(METHOD_NAME);
    }

    public static XxHash_64Method create(Object... args) {
        return new XxHash_64Method();
    }

    @Override
    public IStreamingHasher newHasher() {
        return new XxHash_64Hasher(DEFAULT_HASH_SEED);
    }

    @Override
    public IStreamingHasher getHasher() {
        return xxHash64Hasher;
    }

    @Override
    protected boolean is64Bit() {
        return true;
    }
}
