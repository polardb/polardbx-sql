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
 * @author yudong
 * @since 2023/3/1 17:45
 **/
public class SqlRestartMaster extends SqlCdcCommandBase {

    {
        operator = new SqlRestartMasterOperator();
        sqlKind = SqlKind.RESTART_MASTER;
        keyWord = "RESTART MASTER";
    }

    public SqlRestartMaster(SqlParserPos pos) {
        super(pos);
    }

    public SqlRestartMaster(SqlParserPos pos, SqlNode with) {
        super(pos, with);
    }

    public static class SqlRestartMasterOperator extends SqlCdcCommandOperator {
        public SqlRestartMasterOperator() {
            super(SqlKind.RESTART_MASTER);
        }
    }
}
