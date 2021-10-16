/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import junit.framework.TestCase;

import java.util.List;

public class MySqlInsertTest_30 extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert into update_delete_base_one_db_one_tb ( pk,varchar_test,integer_test,char_test,blob_test,tinyint_test,tinyint_1bit_test,smallint_test,mediumint_test,bit_test,bigint_test,float_test,double_test,decimal_test,date_test,time_test,datetime_test,timestamp_test,year_test) values ( 0, null, 16, 'hello1234', 49, 12, 18, 5, 9, 1, 8, 4.55, 10.2145, 10, '2012-12-13', '12:23:00', '2012-12-13 12:23:00', '2012-12-13 12:23:00', 2012),( 1, 'he343243', 17, 'he343243', 50, 13, 19, 6, 10, 0, 9, 4.56, 21.258, 100, '2013-04-05', '11:23:45', '2014-02-12 11:23:45', '2014-02-12 11:23:45', 2013),( 2, 'word23', 18, 'word23', 51, 14, 20, 7, 11, 1, 10, 4.32, 35.1478, 1000, '2015-11-23', '06:34:12', '2013-04-05 06:34:12', '2013-04-05 06:34:12', 2014),( 3, 'feed32feed', 19, 'feed32feed', 52, 15, 21, 8, 12, 0, 11, 4.23, 38.4879, 10000, '2010-02-22', '08:02:45', '2015-11-23 08:02:45', '2015-11-23 08:02:45', 2015),( 4, 'nihaore', 20, 'nihaore', 53, 16, 22, 9, 13, 1, 12, 5.34, 40.47845, 100000, '2015-12-02', '18:35:23', '2010-02-22 18:35:23', '2010-02-22 18:35:23', 2016),( 5, 'afdaewer', 21, 'afdaewer', 54, 17, 23, 10, 14, 0, 13, 6.78, 48.1478, 1000000, '2014-05-26', '15:23:34', '2014-05-26 20:12:12', '2014-05-26 20:12:12', 2017),( 6, 'hellorew', 22, 'hellorew', 55, 18, 24, 11, 15, 1, 14, 4.51, 50.48745, 10000000, '2011-12-23', '20:12:12', '2011-12-23 12:12:12', '2011-12-23 12:12:12', 2018),( 7, 'abdfeed', 23, 'abdfeed', 56, 19, 25, 12, 16, 0, 15, 7.34, 55.1478, 100000000, '2003-04-05', '12:12:12', '2003-04-05 12:23:34', '2003-04-05 12:23:34', 2019),( 8, 'cdefeed', 24, 'cdefeed', 57, 20, 26, 13, 17, 1, 16, 5.78, 58.1245, 100000000, '2013-02-05', '12:23:34', '2013-02-05 12:27:32', '2013-02-05 12:27:32', 2011),( 9, 'adaabcwer', 25, 'adaabcwer', 48, 21, 27, 14, 18, 0, 17, 0.45, 80.4578, 1000000000, '2013-09-02', '12:27:32', '2013-09-02 14:47:28', '2013-09-02 14:47:28', 2009)";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals(10, insertStmt.getValuesList().size());
        assertEquals(19, insertStmt.getValues().getValues().size());
        assertEquals(19, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String formatSql = "INSERT INTO update_delete_base_one_db_one_tb (pk, varchar_test, integer_test, char_test, blob_test\n" +
                "\t, tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test\n" +
                "\t, bigint_test, float_test, double_test, decimal_test, date_test\n" +
                "\t, time_test, datetime_test, timestamp_test, year_test)\n" +
                "VALUES (0, NULL, 16, 'hello1234', 49\n" +
                "\t\t, 12, 18, 5, 9, 1\n" +
                "\t\t, 8, 4.55, 10.2145, 10, '2012-12-13'\n" +
                "\t\t, '12:23:00', '2012-12-13 12:23:00', '2012-12-13 12:23:00', 2012),\n" +
                "\t(1, 'he343243', 17, 'he343243', 50\n" +
                "\t\t, 13, 19, 6, 10, 0\n" +
                "\t\t, 9, 4.56, 21.258, 100, '2013-04-05'\n" +
                "\t\t, '11:23:45', '2014-02-12 11:23:45', '2014-02-12 11:23:45', 2013),\n" +
                "\t(2, 'word23', 18, 'word23', 51\n" +
                "\t\t, 14, 20, 7, 11, 1\n" +
                "\t\t, 10, 4.32, 35.1478, 1000, '2015-11-23'\n" +
                "\t\t, '06:34:12', '2013-04-05 06:34:12', '2013-04-05 06:34:12', 2014),\n" +
                "\t(3, 'feed32feed', 19, 'feed32feed', 52\n" +
                "\t\t, 15, 21, 8, 12, 0\n" +
                "\t\t, 11, 4.23, 38.4879, 10000, '2010-02-22'\n" +
                "\t\t, '08:02:45', '2015-11-23 08:02:45', '2015-11-23 08:02:45', 2015),\n" +
                "\t(4, 'nihaore', 20, 'nihaore', 53\n" +
                "\t\t, 16, 22, 9, 13, 1\n" +
                "\t\t, 12, 5.34, 40.47845, 100000, '2015-12-02'\n" +
                "\t\t, '18:35:23', '2010-02-22 18:35:23', '2010-02-22 18:35:23', 2016),\n" +
                "\t(5, 'afdaewer', 21, 'afdaewer', 54\n" +
                "\t\t, 17, 23, 10, 14, 0\n" +
                "\t\t, 13, 6.78, 48.1478, 1000000, '2014-05-26'\n" +
                "\t\t, '15:23:34', '2014-05-26 20:12:12', '2014-05-26 20:12:12', 2017),\n" +
                "\t(6, 'hellorew', 22, 'hellorew', 55\n" +
                "\t\t, 18, 24, 11, 15, 1\n" +
                "\t\t, 14, 4.51, 50.48745, 10000000, '2011-12-23'\n" +
                "\t\t, '20:12:12', '2011-12-23 12:12:12', '2011-12-23 12:12:12', 2018),\n" +
                "\t(7, 'abdfeed', 23, 'abdfeed', 56\n" +
                "\t\t, 19, 25, 12, 16, 0\n" +
                "\t\t, 15, 7.34, 55.1478, 100000000, '2003-04-05'\n" +
                "\t\t, '12:12:12', '2003-04-05 12:23:34', '2003-04-05 12:23:34', 2019),\n" +
                "\t(8, 'cdefeed', 24, 'cdefeed', 57\n" +
                "\t\t, 20, 26, 13, 17, 1\n" +
                "\t\t, 16, 5.78, 58.1245, 100000000, '2013-02-05'\n" +
                "\t\t, '12:23:34', '2013-02-05 12:27:32', '2013-02-05 12:27:32', 2011),\n" +
                "\t(9, 'adaabcwer', 25, 'adaabcwer', 48\n" +
                "\t\t, 21, 27, 14, 18, 0\n" +
                "\t\t, 17, 0.45, 80.4578, 1000000000, '2013-09-02'\n" +
                "\t\t, '12:27:32', '2013-09-02 14:47:28', '2013-09-02 14:47:28', 2009)";
        assertEquals(formatSql, SQLUtils.toMySqlString(insertStmt));

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, VisitorFeature.OutputParameterizedQuesUnMergeValuesList);
        System.out.println(psql);
    }
}
