/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.exception;

import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class TddlRuntimeException extends TddlNestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    private final ErrorCode errorCode;

    public TddlRuntimeException(ErrorCode errorCode, String... params) {
        super(errorCode.getMessage(params));
        this.vendorCode = errorCode.getCode();
        this.errorCode = errorCode;
    }

    public TddlRuntimeException(ErrorCode errorCode, Throwable cause, String... params) {
        super(errorCode.getMessage(params), cause);
        this.errorCode = errorCode;
        // 这里因为有 cause , super(xxx)会自动从cause获取vendorCode的值，不需要取errorCode的值
        // 但是如果没有能够自动获取到vendorCode，就还是从errorCode里面取值
        if (this.vendorCode == 0) {
            this.vendorCode = errorCode.getCode();
        }
    }

    @Deprecated
    public TddlRuntimeException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        if (this.vendorCode == 0) {
            this.vendorCode = errorCode.getCode();
        }
    }

    @Override
    public String toString() {
        return super.getLocalizedMessage();
    }

    @Override
    public String getMessage() {
        if (vendorCode > 0) {
            return super.getMessage();
        } else {
            return super.getMessage();
        }
    }

    public ErrorCode getErrorCodeType() {
        return errorCode;
    }
}
