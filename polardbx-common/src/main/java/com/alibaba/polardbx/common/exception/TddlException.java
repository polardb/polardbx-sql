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
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.SQLException;

public class TddlException extends SQLException {

    private static final long serialVersionUID = 1540164086674285095L;

    public TddlException(ErrorCode errorCode, String... params) {
        super(errorCode.getMessage(params), "ERROR", errorCode.getCode());
    }

    public TddlException(ErrorCode errorCode, Throwable cause, String... params) {
        super(errorCode.getMessage(params), getSQLState(cause), getErrorCode(cause, errorCode), cause);
    }

    public TddlException(Throwable cause) {
        super(cause.getMessage(), getSQLState(cause), getErrorCode(cause, null), cause);
    }

    @Override
    public String toString() {
        return getLocalizedMessage();
    }

    @Override
    public String getMessage() {
        if (super.getMessage() != null) {
            return super.getMessage();
        } else {
            Throwable ca = this;
            do {
                Throwable c = ExceptionUtils.getCause(ca);
                if (c != null) {
                    ca = c;
                } else {
                    break;
                }
            } while (ca.getMessage() == null);
            return ca.getMessage();
        }
    }

    private static int getErrorCode(Throwable e, ErrorCode errorCode) {
        if (e instanceof SQLException
            && !(e.getClass() != null && e.getClass().getName().contains("GetConnectionTimeoutException"))) {
            return ((SQLException) e).getErrorCode();
        } else if (e instanceof TddlNestableRuntimeException) {
            return ((TddlNestableRuntimeException) e).getErrorCode();
        }

        if (errorCode != null) {
            return errorCode.getCode();
        } else {
            return 0;
        }
    }

    private static String getSQLState(Throwable e) {
        if (e instanceof SQLException) {
            if (((SQLException) e).getSQLState() != null && ((SQLException) e).getSQLState()
                .equalsIgnoreCase("08S01")) {

                return null;
            } else {
                return ((SQLException) e).getSQLState();
            }
        } else if (e instanceof TddlNestableRuntimeException) {
            return ((TddlNestableRuntimeException) e).getSQLState();
        }

        return null;
    }
}
