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

/**
 * @author jianghang 2013-11-12 下午2:25:55
 * @since 5.0.0
 */
public class SqlParserException extends TddlRuntimeException {

    private static final long serialVersionUID = 6432150590171245275L;

    public SqlParserException(String... params){
        super(ErrorCode.ERR_PARSER, params);
    }

    public SqlParserException(Throwable cause, String... params){
        super(ErrorCode.ERR_PARSER, cause, params);
    }

    public SqlParserException(Throwable cause){
        super(ErrorCode.ERR_PARSER, cause, new String[] { cause.getMessage() });
    }

}
