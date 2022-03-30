package com.alibaba.polardbx.common.scheduler;

public enum MisfirePolicy {

    /**
     * compensate all misfires
     */
    COMPENSATE,
    /**
     * ignore all misfires
     */
    IGNORE
    ;

}