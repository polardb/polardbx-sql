package com.alibaba.polardbx.common.encdb;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author pangzhaoxing
 */
public class EncdbException extends TddlRuntimeException {

    public EncdbException(String... params) {
        super(ErrorCode.ERR_ENCDB, params);
    }

    public EncdbException(String param, Throwable e) {
        super(ErrorCode.ERR_ENCDB, param, e);
    }

    public EncdbException(Throwable e) {
        super(ErrorCode.ERR_ENCDB, e.getMessage(), e);
    }

    public EncdbException(ErrorCode errorCode, String... params) {
        super(errorCode, params);
    }
}
