package com.alibaba.polardbx.executor.ddl.job.task.ttl.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author chenghui.lch
 */
public class TtlJobRuntimeException extends TddlRuntimeException {

    public TtlJobRuntimeException(String... params) {
        super(ErrorCode.ERR_TTL, params);
    }

    public TtlJobRuntimeException(Throwable ex, String... params) {
        super(ErrorCode.ERR_TTL, ex, params);
    }
}
