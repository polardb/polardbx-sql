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

package com.alibaba.polardbx.druid.sql.transform;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLType;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.StringUtils;

import static com.alibaba.polardbx.druid.sql.parser.SQLParserFeature.EnableSQLBinaryOpExprGroup;

/**
 * @author lijun.cailj 2020/5/8
 */
public class SQLUnifiedUtils {

    public static long unifyHash(String sql, DbType type) {
        String unifySQL = unifySQL(sql, DbType.mysql);
        return FnvHash.fnv1a_64_lower(unifySQL);
    }

    public static String unifySQL(String sql, DbType type) {
        if (StringUtils.isEmpty(sql)) {
            throw new IllegalArgumentException("sql is empty.");
        }

        SQLType sqlType = SQLParserUtils.getSQLType(sql, DbType.mysql);

        String parameterizeSQL = null;
        switch (sqlType) {
            case INSERT:
            case UPDATE:
            case SELECT:
            case DELETE:
                parameterizeSQL = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql);
                SQLStatement stmt = SQLUtils.parseSingleStatement(parameterizeSQL, DbType.mysql, EnableSQLBinaryOpExprGroup);
                stmt.accept(new SQLUnifiedVisitor());
                return SQLUtils.toMySqlString(stmt);
            default:
                return ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql);
        }
    }
}
