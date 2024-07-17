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

package com.alibaba.polardbx.optimizer.utils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

/**
 * @author chenghui.lch
 */
public class SqlIdentifierUtil {

    /**
     * Convert a unescaped string of identifier to an escaped string
     * <pre>
     *     e.g
     *      i`D （unescaped string）  ==> `i``D` （escaped string）
     * </pre>
     */
    public static String escapeIdentifierString(String unescapedIdString) {
        /**
         * 1.
         *  Convert a unescapedIdString without escaped symbols to SqlIdentifier;
         * 2.
         *  Convert a SqlIdentifier to a escaped String with handling escaped symbols
         *  which is been wrapped with backquotes(`).
         */
        SqlIdentifier idAst = new SqlIdentifier(unescapedIdString, SqlParserPos.ZERO);
        SqlString idSqlStr = idAst.toSqlString(MysqlSqlDialect.DEFAULT);
        String idEscapedStr = idSqlStr.getSql();
        return idEscapedStr;
    }

    /**
     * Convert a sqlnode of identifier to an escaped string
     * <pre>
     *     e.g
     *      i`D （unescaped string）  ==> `i``D` （escaped string）
     * </pre>
     */
    public static String escapeIdentifierStringByAst(SqlNode idAst) {
        /**
         * Use MysqlSqlDialect to convert a SqlNode
         * to string and handle all escaped symbols.
         * The escaped string has been wrapped with backquotes(`).
         */
        SqlString idAstSqlStr = idAst.toSqlString(MysqlSqlDialect.DEFAULT);
        String escapedStr = idAstSqlStr.toString();
        return escapedStr;
    }
}
