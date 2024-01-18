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

import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.List;



/**
 * @author shicai.xsc 2021/3/5 13:09
 * @desc
 * @since 5.0.0.0
 */
public class SqlChangeReplicationFilter extends SqlReplicationBase {

    {
        operator = new SqlChangeReplicationFilterOperator();
        sqlKind = SqlKind.CHANGE_REPLICATION_FILTER;
        keyWord = "CHANGE REPLICATION FILTER";
    }

    public SqlChangeReplicationFilter(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel,
                                      SqlNode subChannel) {
        super(pos, options, channel, subChannel);
    }


    @Override
    protected void parseParams(String k, String v) {
        k = StringUtils.upperCase(k);
        params.put(k, v);
    }

    public static class SqlChangeReplicationFilterOperator extends SqlReplicationOperator {

        public SqlChangeReplicationFilterOperator() {
            super(SqlKind.CHANGE_REPLICATION_FILTER);
        }
    }
}
