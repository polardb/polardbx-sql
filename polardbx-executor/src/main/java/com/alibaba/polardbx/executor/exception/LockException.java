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

package com.alibaba.polardbx.executor.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * Created by guoguan on 15-4-13.
 */
public class LockException extends TddlRuntimeException {

    private static final long serialVersionUID = 5754269584847245223L;

    public LockException(ErrorCode errorCode, String... params) {
        super(errorCode, params);
    }

    public LockException(ErrorCode errorCode, Throwable cause, String... params) {
        super(errorCode, cause, params);
    }

    public LockException(Throwable cause) {
        super(ErrorCode.ERR_OTHER, cause, new String[] {cause.getMessage()});
    }
}
