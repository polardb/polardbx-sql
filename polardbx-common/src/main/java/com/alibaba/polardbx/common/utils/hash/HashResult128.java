package com.alibaba.polardbx.common.utils.hash;

public class HashResult128 {
    long result1;
    long result2;

    public long getResult1() {
        return result1;
    }

    public long getResult2() {
        return result2;
    }

    /**
     * 返回前8字节结果
     */
    public long asLong() {
        return result1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HashResult128 that = (HashResult128) o;
        return result1 == that.result1 && result2 == that.result2;
    }

    @Override
    public int hashCode() {
        return (int) (result1 ^ result2);
    }
}