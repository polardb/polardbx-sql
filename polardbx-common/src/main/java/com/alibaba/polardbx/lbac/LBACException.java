package com.alibaba.polardbx.lbac;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class LBACException extends TddlRuntimeException {

    public LBACException(String... params) {
        super(ErrorCode.ERR_LBAC, params);
    }

    public LBACException(String param, Throwable e) {
        super(ErrorCode.ERR_LBAC, param, e);
    }

    public LBACException(Throwable e) {
        super(ErrorCode.ERR_LBAC, e.getMessage(), e);
    }

}
