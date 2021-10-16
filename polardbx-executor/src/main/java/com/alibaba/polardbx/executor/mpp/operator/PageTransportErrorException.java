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

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class PageTransportErrorException extends TddlRuntimeException {

    public PageTransportErrorException(String message) {
        super(ErrorCode.ERR_PAGE_TRANSPORT_ERROR, message);
    }

    public PageTransportErrorException(String message, Throwable cause) {
        super(ErrorCode.ERR_PAGE_TRANSPORT_ERROR, cause, message);
    }
}
