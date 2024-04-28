package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.utils.GeneralUtil;

public interface CastableBlock {
    default <T extends CastableBlock> T cast(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        throw GeneralUtil.nestedException(new ClassCastException(
            "failed to cast " + this.getClass().getName() + " to " + clazz.getName()));
    }

    default boolean isInstanceOf(Class clazz) {
        return clazz.isInstance(this);
    }
}
