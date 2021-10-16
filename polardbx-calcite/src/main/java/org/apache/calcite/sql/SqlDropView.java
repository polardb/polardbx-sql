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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author dylan
 * @date 2020/3/3 6:43 PM
 */
public class SqlDropView extends SqlDropObject {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP VIEW", SqlKind.DROP_VIEW);

    public SqlDropView(SqlIdentifier name, boolean ifExists) {
        super(OPERATOR, SqlParserPos.ZERO, ifExists, name);
    }

    public SqlIdentifier getName() {
        return (SqlIdentifier) name;
    }
}
