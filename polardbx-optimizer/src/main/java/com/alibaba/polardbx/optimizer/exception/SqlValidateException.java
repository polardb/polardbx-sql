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

package com.alibaba.polardbx.optimizer.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_VALIDATE;

/**
 * @author lingce.ldm 2017-07-05 19:52
 */
public class SqlValidateException extends TddlRuntimeException {

    private static final long serialVersionUID = 5169636057962021132L;

    public SqlValidateException(String... params) {
        super(ErrorCode.ERR_PARSER, params);
    }

    public SqlValidateException(Throwable cause, String... params) {
        super(ErrorCode.match(cause.getMessage(), ERR_VALIDATE), cause, params);
    }

    public SqlValidateException(Throwable cause) {
        super(ErrorCode.match(cause.getMessage(), ERR_VALIDATE), cause, new String[] {cause.getMessage()});
    }
}
