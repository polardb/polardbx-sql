package com.alibaba.polardbx.gms.node;

public enum MppScope {

    /**
     * 当前实例
     */
    CURRENT,

    /**
     * 远程行存只读实例
     */
    SLAVE,

    /**
     * 远程列存只读实例
     */
    COLUMNAR,

    ALL
}
