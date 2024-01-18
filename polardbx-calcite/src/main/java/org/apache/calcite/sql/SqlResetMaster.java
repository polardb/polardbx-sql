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
 * @since 2023/3/1 17:51
 **/
public class SqlResetMaster extends SqlCdcCommandBase {

    {
        operator = new SqlResetMasterOperator();
        sqlKind = SqlKind.RESET_MASTER;
        keyWord = "RESET MASTER";
    }

    public SqlResetMaster(SqlParserPos pos) {
        super(pos);
    }

    public SqlResetMaster(SqlParserPos pos, SqlNode with) {
        super(pos, with);
    }

    public static class SqlResetMasterOperator extends SqlCdcCommandOperator {
        public SqlResetMasterOperator() {
            super(SqlKind.RESET_MASTER);
        }
    }
}
