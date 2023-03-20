package com.alibaba.polardbx.optimizer.json.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class JsonPathNotFoundException extends TddlRuntimeException {
    public JsonPathNotFoundException(String path) {
        super(ErrorCode.ERR_PARSER, "JSON path is not found: " + path);
    }
}
