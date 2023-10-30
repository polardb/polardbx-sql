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
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @Author ShuGuang
 * @Description
 * @Date 2021/3/4 11:41 上午
 */

public class SqlChangeMaster extends SqlReplicationBase {

    {
        operator = new SqlChangeMasterOperator();
        sqlKind = SqlKind.CHANGE_MASTER;
        keyWord = "CHANGE MASTER TO";
    }

    public SqlChangeMaster(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel,
                           SqlNode subChannel) {
        super(pos, options, channel, subChannel);
    }

    @Override
    protected void parseParams(String k, String v) {
        k = StringUtils.upperCase(k);
        params.put(k, v);
    }

    public static class SqlChangeMasterOperator extends SqlReplicationOperator {

        public SqlChangeMasterOperator() {
            super(SqlKind.CHANGE_MASTER);
        }
    }
}