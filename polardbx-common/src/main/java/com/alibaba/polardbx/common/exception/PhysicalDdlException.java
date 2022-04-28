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

/**
 * @author guxu
 */
public class PhysicalDdlException extends TddlRuntimeException {

    private static final long serialVersionUID = 3233001737716650503L;

    private int totalCount;
    private int successCount;
    private int failCount;
    private String errMsg;
    private String simpleErrMsg;

    public PhysicalDdlException(int totalCount, int successCount, int failCount, String errMsg, String simpleErrMsg){
        super(ErrorCode.ERR_DDL_JOB_ERROR, errMsg);
        this.totalCount = totalCount;
        this.successCount = successCount;
        this.failCount = failCount;
        this.errMsg = errMsg;
        this.simpleErrMsg = simpleErrMsg;
    }

    public int getTotalCount() {
        return this.totalCount;
    }


    public int getSuccessCount() {
        return this.successCount;
    }


    public int getFailCount() {
        return this.failCount;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public String getSimpleErrMsg() {
        return this.simpleErrMsg;
    }
}
