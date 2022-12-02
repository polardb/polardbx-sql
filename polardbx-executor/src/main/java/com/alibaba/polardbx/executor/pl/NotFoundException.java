package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class NotFoundException extends TddlRuntimeException {
    public NotFoundException() {
        super(ErrorCode.ERR_DATA_NOT_FOUND);
    }
}
