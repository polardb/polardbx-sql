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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @Author ShuGuang
 * @Description
 * @Date 2020/10/20 3:30 下午
 */
public class SqlShowBinaryLogs extends SqlShow {
    private static final List<SqlNode> OPERANDS_EMPTY = new ArrayList<>(0);
    private static final List<SqlSpecialIdentifier> SPECIAL_IDENTIFIERS = Lists.newArrayList(
        SqlSpecialIdentifier.BINARY,
        SqlSpecialIdentifier.LOGS);

    public SqlShowBinaryLogs(SqlParserPos pos) {
        super(pos, SPECIAL_IDENTIFIERS, OPERANDS_EMPTY, null, null, null, null);
    }

    public static SqlShowBinaryLogs create(SqlParserPos pos) {
        return new SqlShowBinaryLogs(pos);
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_BINARY_LOGS;
    }

}
