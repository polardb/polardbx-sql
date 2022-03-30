package com.alibaba.polardbx.common.utils.hash;

import java.nio.charset.StandardCharsets;

/**
 * 按Block直接hash, 非流式
 * 操作均无状态
 * 避免内存拷贝
 */
public interface IBlockHasher {
    default HashResult128 hashByte(byte b) {
        return hashLong(b);
    }

    default HashResult128 hashShort(short s) {
        return hashLong(s);
    }

    default HashResult128 hashInt(int i) {
        return hashLong(i);
    }

    HashResult128 hashLong(long l);

    default HashResult128 hashDouble(double d) {
        return hashLong(Double.doubleToRawLongBits(d));
    }

    HashResult128 hashBytes(byte[] bytes);

    default HashResult128 hashString(String str) {
        // TODO charset
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        return hashBytes(data);
    }
}
