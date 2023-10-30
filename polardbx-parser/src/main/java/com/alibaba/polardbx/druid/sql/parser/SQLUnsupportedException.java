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

package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.FastsqlException;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnsupportedStatement;

public class SQLUnsupportedException extends FastsqlException {

    private static final long serialVersionUID = 1L;

    public static final String UNSUPPORTED_SQL_WARN = "unsupported sql : ";

    public SQLUnsupportedException(SQLUnsupportedStatement sqlUnsupportedStatement) {
        super(UNSUPPORTED_SQL_WARN + sqlUnsupportedStatement.toString());
    }

    public SQLUnsupportedException(ByteString sql) {
        super(UNSUPPORTED_SQL_WARN + sql.toString());
    }

    public SQLUnsupportedException(String sql) {
        super(UNSUPPORTED_SQL_WARN + sql);
    }

}
