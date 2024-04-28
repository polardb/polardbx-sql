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
