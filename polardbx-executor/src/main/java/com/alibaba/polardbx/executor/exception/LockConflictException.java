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

import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * Created by guoguan on 15-4-13.
 */
public class LockConflictException extends LockNeedRetryException {

    private static final long serialVersionUID = 5257065721618839000L;

    public LockConflictException(ErrorCode errorCode, String... params) {
        super(errorCode, params);
    }

    public LockConflictException(ErrorCode errorCode, Throwable cause, String... params) {
        super(errorCode, cause, params);
    }

    public LockConflictException(Throwable cause) {
        super(cause);
    }
}
