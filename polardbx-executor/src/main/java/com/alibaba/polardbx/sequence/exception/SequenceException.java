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

package com.alibaba.polardbx.sequence.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author jianghang 2014-4-23 下午3:44:45
 * @since 5.1.0
 */
public class SequenceException extends TddlRuntimeException {

    private static final long serialVersionUID = -7383087459057215862L;

    public SequenceException(String... params) {
        super(ErrorCode.ERR_SEQUENCE, params);
    }

    public SequenceException(Throwable cause, String... params) {
        super(ErrorCode.ERR_SEQUENCE, cause, params);
    }

    public SequenceException(Throwable cause) {
        super(ErrorCode.ERR_SEQUENCE, cause);
    }
}
