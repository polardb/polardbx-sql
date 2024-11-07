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

package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.sql.Connection;

public enum ShareReadViewPolicy {
    ON,
    OFF,

    DEFAULT;

    public static boolean supportTxIsolation(int isolationLevel) {
        return isolationLevel == Connection.TRANSACTION_REPEATABLE_READ ||
            (DynamicConfig.getInstance().isEnableShareReadviewInRc()
                && isolationLevel == Connection.TRANSACTION_READ_COMMITTED);
    }

    public static void checkTxIsolation(int isolationLevel) {
        if (!supportTxIsolation(isolationLevel)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "Share read view is only supported in repeatable-read. "
                    + "Or in read-committed when ENABLE_SHARE_READVIEW_IN_RC is true.");
        }
    }
}
