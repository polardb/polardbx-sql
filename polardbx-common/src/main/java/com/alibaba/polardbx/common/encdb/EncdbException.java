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
