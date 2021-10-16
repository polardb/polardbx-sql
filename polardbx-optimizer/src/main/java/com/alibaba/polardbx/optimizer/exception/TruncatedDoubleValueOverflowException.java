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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;

/**
 * Truncated incorrect DOUBLE value超过列值范围
 *
 * @author arnkore 2016-09-07 16:38
 */
public class TruncatedDoubleValueOverflowException extends TddlNestableRuntimeException {

    public TruncatedDoubleValueOverflowException() {
    }

    public TruncatedDoubleValueOverflowException(String msg) {
        super(msg);
    }

    public TruncatedDoubleValueOverflowException(Throwable cause) {
        super(cause);
    }

    public TruncatedDoubleValueOverflowException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TruncatedDoubleValueOverflowException(Throwable cause, Object extraParams) {
        super(cause, extraParams);
    }
}
