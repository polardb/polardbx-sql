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

package com.taobao.tddl.common.privilege;

import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.io.Serializable;

public class AuthorityRpcResult implements Serializable {
    private static final long serialVersionUID = 3160278047019899841L;

    private ErrorCode code;

    private String logMessage;

    private String[] params;

    public AuthorityRpcResult(ErrorCode code) {
        this(code, null, null);
    }

    public AuthorityRpcResult(ErrorCode code, String[] params) {
        this(code, null, params);
    }

    public AuthorityRpcResult(ErrorCode code, String logMessage, String[] params) {
        this.code = code;
        this.logMessage = logMessage;
        this.params = params;
    }

    public static AuthorityRpcResult createSuccessResult() {
        return new AuthorityRpcResult(ErrorCode.AUTHORITY_SUCCESS);
    }

    public static AuthorityRpcResult createCommonSystemErrorResult(String exceptionMsg) {
        return new AuthorityRpcResult(ErrorCode.AUTHORITY_COMMON_EXCEPTION, exceptionMsg, null);
    }

    public ErrorCode getCode() {
        return code;
    }

    public void setCode(ErrorCode code) {
        this.code = code;
    }

    public String getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(String logMessage) {
        this.logMessage = logMessage;
    }

    public String[] getParams() {
        return params;
    }

    public void setParams(String[] params) {
        this.params = params;
    }
}
