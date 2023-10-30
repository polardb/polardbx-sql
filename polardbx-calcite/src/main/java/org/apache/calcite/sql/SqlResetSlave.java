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
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * @author shicai.xsc 2021/5/26 16:30
 * @since 5.0.0.0
 */
public class SqlResetSlave extends SqlReplicationBase {

    private boolean isAll;

    {
        operator = new SqlResetSlaveOperator();
        sqlKind = SqlKind.RESET_SLAVE;
        keyWord = "RESET SLAVE";
    }

    public SqlResetSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel,
                         SqlNode subChannel, boolean isAll) {
        super(pos, options, channel, subChannel);
        this.isAll = isAll;
    }

    public static class SqlResetSlaveOperator extends SqlReplicationOperator {

        public SqlResetSlaveOperator() {
            super(SqlKind.RESET_SLAVE);
        }
    }

    public boolean isAll() {
        return isAll;
    }
}