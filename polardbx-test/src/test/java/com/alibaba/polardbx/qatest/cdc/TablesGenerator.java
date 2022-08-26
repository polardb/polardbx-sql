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

package com.alibaba.polardbx.qatest.cdc;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Sets;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * created by ziyang.lb
 **/
public class TablesGenerator {

    public final static SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };

    public static void main(String args[]) throws SQLException {
        CdcCheckTest cdcCheckTest = new CdcCheckTest();
        cdcCheckTest.before();

        Set<String> srcTables = new HashSet<>(cdcCheckTest.getTableList("slt", 0));
        Set<String> dstTables = new HashSet<>(cdcCheckTest.getTableList("slt", 1));
        Set<String> sets = Sets.difference(srcTables, dstTables);
        int count = 0;
        for (String s : sets) {
            String table = "`slt`.`" + s + "`";
            String sql = cdcCheckTest.getCreateTableSql(table);
            System.out.println("drop table if exists " + table + ";");
            System.out.println(transfer(sql) + ";");
            System.out.println();
            count++;
        }
        System.out.println("table count is " + count);
    }

    private static String transfer(String sql) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement sqlStatement = statementList.get(0);
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) sqlStatement;
            createTableStatement.setBroadCast(false);
            createTableStatement.setPartitioning(null);
            createTableStatement.setDbPartitionBy(null);
            createTableStatement.setDbPartitions(null);
            createTableStatement.setExPartition(null);
            createTableStatement.setTablePartitionBy(null);
            createTableStatement.setTablePartitions(null);
            createTableStatement.setPrefixBroadcast(false);
            createTableStatement.setPrefixPartition(false);
            createTableStatement.setTableGroup(null);

            List<SQLTableElement> sqlTableElementList = createTableStatement.getTableElementList();
            Iterator<SQLTableElement> it = sqlTableElementList.iterator();
            while (it.hasNext()) {
                SQLTableElement el = it.next();
                if (el instanceof SQLColumnDefinition) {
                    SQLColumnDefinition definition = (SQLColumnDefinition) el;
                    definition.setSequenceType(null);
                    definition.setComment("");
                }
                if (el instanceof MySqlTableIndex) {
                    MySqlTableIndex index = (MySqlTableIndex) el;
                    if (index.getIndexDefinition().isGlobal()) {
                        it.remove();
                    }
                }
                if (el instanceof MySqlPrimaryKey) {
                    MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) el;
                    SQLIndexDefinition indexDefinition = primaryKey.getIndexDefinition();
                }
            }

            return createTableStatement.toString();
        }

        return sql;
    }
}
