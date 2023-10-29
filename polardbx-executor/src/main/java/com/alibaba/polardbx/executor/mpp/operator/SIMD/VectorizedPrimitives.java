package com.alibaba.polardbx.executor.mpp.operator.SIMD;

public interface VectorizedPrimitives {
    VectorizedPrimitives SCALAR_PRIMITIVES_HANDLER = new ScalarVectorizedPrimitives();
    VectorizedPrimitives SIMD_PRIMITIVES_HANDLER = new SIMDVectorizedPrimitives(SCALAR_PRIMITIVES_HANDLER);
    static VectorizedPrimitives get(boolean usingSIMD)
    {
        return usingSIMD ? SIMD_PRIMITIVES_HANDLER : SCALAR_PRIMITIVES_HANDLER;
    }
    void gather(byte[] source, int[] indexes, int start, int count, byte[] target, int targetOffset);

    void gather(int[] source, int[] indexes, int start, int count, int[] target, int targetOffset);

    void gather(long[] source, int[] indexes, int start, int count, long[] target, int targetOffset);

    default void gather(byte[] source, int[] indexes, byte[] target, int targetOffset) {
        gather(source, indexes, 0, indexes.length, target, targetOffset);
    }

    default void gather(int[] source, int[] indexes, int[] target, int targetOffset) {
        gather(source, indexes, 0, indexes.length, target, targetOffset);
    }

    default void gather(long[] source, int[] indexes, long[] target, int targetOffset) {
        gather(source, indexes, 0, indexes.length, target, targetOffset);
    }

    @Deprecated
    default void gather_native(byte[] source, int[] indexes, int start, int count, byte[] target, int targetOffset) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName());
    }

    @Deprecated
    default void gather_native(int[] source, int[] indexes, int start, int count, int[] target, int targetOffset) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName());
    }

    @Deprecated
    default void gather_native(long[] source, int[] indexes, int start, int count, long[] target, int targetOffset) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName());
    }

    @Deprecated
    default void gather_native(byte[] source, int[] indexes, byte[] target, int targetOffset) {
        gather_native(source, indexes, 0, indexes.length, target, targetOffset);
    }

    @Deprecated
    default void gather_native(int[] source, int[] indexes, int[] target, int targetOffset) {
        gather_native(source, indexes, 0, indexes.length, target, targetOffset);
    }

    @Deprecated
    default void gather_native(long[] source, int[] indexes, long[] target, int targetOffset) {
        gather_native(source, indexes, 0, indexes.length, target, targetOffset);
    }

    void scatter(long[] source, long[] target, int[] scatterMap, int offset, int copySize);

    void scatter(int[] source, int[] target, int[] scatterMap, int offset, int copySize);

    void scatter(short[] source, short[] target, int[] scatterMap, int offset, int copySize);

    void scatter(byte[] source, byte[] target, int[] scatterMap, int offset, int copySize);

    void scatter(boolean[] source, boolean[] target, int[] scatterMap, int offset, int copySize);
}
