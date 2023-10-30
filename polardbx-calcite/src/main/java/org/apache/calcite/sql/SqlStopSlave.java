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

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/**
 * @author shicai.xsc 2021/3/5 13:08
 * @desc
 * @since 5.0.0.0
 */
public class SqlStopSlave extends SqlReplicationBase {

    {
        operator = new SqlStopSlaveOperator();
        sqlKind = SqlKind.STOP_SLAVE;
        keyWord = "STOP SLAVE";
    }

    public SqlStopSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel, SqlNode subChannel){
        super(pos, options, channel, subChannel);
    }

    public static class SqlStopSlaveOperator extends SqlReplicationOperator {

        public SqlStopSlaveOperator(){
            super(SqlKind.STOP_SLAVE);
        }
    }
}
