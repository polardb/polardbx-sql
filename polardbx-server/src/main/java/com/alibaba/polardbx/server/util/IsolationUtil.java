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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import org.apache.calcite.sql.SqlSetTransaction;

/**
 * @author zhuangtianyi
 */
public class IsolationUtil {
    public static IsolationLevel convertFastSql(SQLStartTransactionStatement.IsolationLevel fastSqlLevel) {
        if (fastSqlLevel == null) {
            return null;
        }
        switch (fastSqlLevel) {
        case SERIALIZABLE:
            return IsolationLevel.SERIALIZABLE;
        case READ_COMMITTED:
            return IsolationLevel.READ_COMMITTED;
        case REPEATABLE_READ:
            return IsolationLevel.REPEATABLE_READ;
        case READ_UNCOMMITTED:
            return IsolationLevel.READ_UNCOMMITTED;
        default:
            assert false;
            return null;
        }
    }

    public static IsolationLevel convertCalcite(SqlSetTransaction.IsolationLevel level) {
        if (level == null) {
            return null;
        }
        switch (level) {
        case READ_UNCOMMITTED:
            return IsolationLevel.READ_UNCOMMITTED;
        case READ_COMMITTED:
            return IsolationLevel.READ_COMMITTED;
        case REPEATABLE_READ:
            return IsolationLevel.REPEATABLE_READ;
        case SERIALIZABLE:
            return IsolationLevel.SERIALIZABLE;
        default:
            assert false;
            return null;
        }
    }
}
