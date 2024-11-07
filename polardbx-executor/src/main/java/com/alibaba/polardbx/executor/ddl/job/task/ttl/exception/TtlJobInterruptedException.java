package com.alibaba.polardbx.executor.ddl.job.task.ttl.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author chenghui.lch
 */
public class TtlJobInterruptedException extends TddlRuntimeException {
    public TtlJobInterruptedException(String... params) {
        super(ErrorCode.ERR_TTL_INTERRUPTED, params);
    }

    public TtlJobInterruptedException(Throwable ex, String... params) {
        super(ErrorCode.ERR_TTL_INTERRUPTED, ex, params);
    }
}
