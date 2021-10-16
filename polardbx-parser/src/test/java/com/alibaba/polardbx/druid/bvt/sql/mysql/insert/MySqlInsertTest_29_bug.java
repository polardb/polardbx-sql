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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;

import java.util.List;

public class MySqlInsertTest_29_bug extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert into test_sub_rt VALUES (3999,0,-100,-91,-105,-263,'15','15',4.12,'safdwe','2012-12-13','09:12:28','2013-03-22 09:17:28.0',72,85.1235,90.111,48.1478,20140630),(4000,null,-59,10,534,-262,'3,9,15,0,9,9,12,3','3,9,15,0,9,9,12,3',5.34,'abdfeed',null,'12:27:32',null,100,85.1234,null,200.48787,20140630),(4001,1,3,null,425,null,'4,13,19,2,16,9,7','4,13,19,2,16,9,7',4.56,'cdefeed','2017-03-22','15:23:34','2015-11-23 08:02:45.0',-45,85.4568,90.1222,180.457845,20140630),(4002,1,113,-70,578,-233,'10,9,1,0,17,18,19,17','10,9,1,0,17,18,19,17',45.23,'afsabcabcd','2017-03-22','20:12:12','2011-06-22 09:12:28.0',-29,85.4147,90.4574,1414.14747,20140630),(4003,0,-75,65,-506,-395,'14','14',7.34,'he343243','2011-06-22','12:23:00','2014-02-12 11:23:45.0',-47,85.1748,90.1476,1825.47484,20140630),(4004,1,-58,-5,-662,-707,'7,15,12,3,8,3,16,10,2','7,15,12,3,8,3,16,10,2',null,'word23','2011-12-23','06:34:12','2011-12-23 12:12:12.0',14,85.1748,91.1235,38.4879,20140630),(4005,0,93,null,670,-23,'15,6,7,4,6,16','15,6,7,4,6,16',5.34,'sfdeiekd',null,'20:12:12','2013-09-02 14:47:28.0',76,86.252,90.1458,55.1478,20140630),(4006,1,null,76,908,null,'2,17,2,10,15,9,3,5','2,17,2,10,15,9,3,5',2.34,'word23','2014-02-12','11:23:45','2013-02-05 12:27:32.0',-89,86.1453,90.1476,180.457845,20140630),(4007,1,106,null,-3,-204,'15,1,2,18,14,5,1','15,1,2,18,14,5,1',null,'adaabcwer','2013-03-22','11:23:45','2017-03-22 07:47:28.0',-68,84.1245,90.1222,90.4587447,20140630),(4008,1,9,11,-379,239,'17,15,5,18,12','17,15,5,18,12',4.51,'afsabcabcd','2013-02-05','06:34:12','2010-02-22 18:35:23.0',null,85.4568,91.1235,58.1245,20140630),(4009,null,41,-99,-510,-770,'18,3,12,12','18,3,12,12',4.51,'cdefeed','2017-03-22','08:02:45','2011-06-22 09:12:28.0',null,85.4568,90.111,40.47845,20140630)";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals(11, insertStmt.getValuesList().size());
        assertEquals(18, insertStmt.getValues().getValues().size());
        assertEquals(0, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String formatSql = "INSERT INTO test_sub_rt\n" + "VALUES (3999, 0, -100, -91, -105\n"
                           + "\t\t, -263, '15', '15', 4.12, 'safdwe'\n"
                           + "\t\t, '2012-12-13', '09:12:28', '2013-03-22 09:17:28.0', 72, 85.1235\n"
                           + "\t\t, 90.111, 48.1478, 20140630),\n" + "\t(4000, NULL, -59, 10, 534\n"
                           + "\t\t, -262, '3,9,15,0,9,9,12,3', '3,9,15,0,9,9,12,3', 5.34, 'abdfeed'\n"
                           + "\t\t, NULL, '12:27:32', NULL, 100, 85.1234\n" + "\t\t, NULL, 200.48787, 20140630),\n"
                           + "\t(4001, 1, 3, NULL, 425\n"
                           + "\t\t, NULL, '4,13,19,2,16,9,7', '4,13,19,2,16,9,7', 4.56, 'cdefeed'\n"
                           + "\t\t, '2017-03-22', '15:23:34', '2015-11-23 08:02:45.0', -45, 85.4568\n"
                           + "\t\t, 90.1222, 180.457845, 20140630),\n" + "\t(4002, 1, 113, -70, 578\n"
                           + "\t\t, -233, '10,9,1,0,17,18,19,17', '10,9,1,0,17,18,19,17', 45.23, 'afsabcabcd'\n"
                           + "\t\t, '2017-03-22', '20:12:12', '2011-06-22 09:12:28.0', -29, 85.4147\n"
                           + "\t\t, 90.4574, 1414.14747, 20140630),\n" + "\t(4003, 0, -75, 65, -506\n"
                           + "\t\t, -395, '14', '14', 7.34, 'he343243'\n"
                           + "\t\t, '2011-06-22', '12:23:00', '2014-02-12 11:23:45.0', -47, 85.1748\n"
                           + "\t\t, 90.1476, 1825.47484, 20140630),\n" + "\t(4004, 1, -58, -5, -662\n"
                           + "\t\t, -707, '7,15,12,3,8,3,16,10,2', '7,15,12,3,8,3,16,10,2', NULL, 'word23'\n"
                           + "\t\t, '2011-12-23', '06:34:12', '2011-12-23 12:12:12.0', 14, 85.1748\n"
                           + "\t\t, 91.1235, 38.4879, 20140630),\n" + "\t(4005, 0, 93, NULL, 670\n"
                           + "\t\t, -23, '15,6,7,4,6,16', '15,6,7,4,6,16', 5.34, 'sfdeiekd'\n"
                           + "\t\t, NULL, '20:12:12', '2013-09-02 14:47:28.0', 76, 86.252\n"
                           + "\t\t, 90.1458, 55.1478, 20140630),\n" + "\t(4006, 1, NULL, 76, 908\n"
                           + "\t\t, NULL, '2,17,2,10,15,9,3,5', '2,17,2,10,15,9,3,5', 2.34, 'word23'\n"
                           + "\t\t, '2014-02-12', '11:23:45', '2013-02-05 12:27:32.0', -89, 86.1453\n"
                           + "\t\t, 90.1476, 180.457845, 20140630),\n" + "\t(4007, 1, 106, NULL, -3\n"
                           + "\t\t, -204, '15,1,2,18,14,5,1', '15,1,2,18,14,5,1', NULL, 'adaabcwer'\n"
                           + "\t\t, '2013-03-22', '11:23:45', '2017-03-22 07:47:28.0', -68, 84.1245\n"
                           + "\t\t, 90.1222, 90.4587447, 20140630),\n" + "\t(4008, 1, 9, 11, -379\n"
                           + "\t\t, 239, '17,15,5,18,12', '17,15,5,18,12', 4.51, 'afsabcabcd'\n"
                           + "\t\t, '2013-02-05', '06:34:12', '2010-02-22 18:35:23.0', NULL, 85.4568\n"
                           + "\t\t, 91.1235, 58.1245, 20140630),\n" + "\t(4009, NULL, 41, -99, -510\n"
                           + "\t\t, -770, '18,3,12,12', '18,3,12,12', 4.51, 'cdefeed'\n"
                           + "\t\t, '2017-03-22', '08:02:45', '2011-06-22 09:12:28.0', NULL, 85.4568\n"
                           + "\t\t, 90.111, 40.47845, 20140630)";
        assertEquals(formatSql, SQLUtils.toMySqlString(insertStmt));
    }
}
