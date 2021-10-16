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
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

public class LoadDataInFileSyntaxTest extends TestCase {

    public void test_0() throws Exception {
        String sql = "LOAD DATA INFILE 'data.txt' INTO TABLE db2.my_table;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA INFILE 'data.txt' INTO TABLE db2.my_table;", text);
    }

    public void test_1() throws Exception {
        String sql = "LOAD DATA INFILE '/tmp/test.txt' INTO TABLE test FIELDS TERMINATED BY ','  LINES STARTING BY 'xxx';";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA INFILE '/tmp/test.txt' INTO TABLE test COLUMNS TERMINATED BY ',' LINES STARTING BY 'xxx';",
                            text);
    }

    public void test_2() throws Exception {
        String sql = "LOAD DATA INFILE '/home/Order.txt' INTO TABLE Orders (Order_Number, Order_Date, Customer_ID);";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA INFILE '/home/Order.txt' INTO TABLE Orders (Order_Number, Order_Date, Customer_ID);",
                            text);
    }

    public void test_3() throws Exception {
        String sql = "LOAD DATA INFILE 'data.txt' INTO TABLE tbl_name FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA INFILE 'data.txt' INTO TABLE tbl_name COLUMNS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;",
                            text);
    }

    public void test_4() throws Exception {
        String sql = "LOAD DATA INFILE 'data.txt' INTO TABLE tbl_name FIELDS TERMINATED BY 0x01 ENCLOSED BY '\"' LINES STARTING BY x'002' TERMINATED BY x'003' IGNORE 1 LINES;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA INFILE 'data.txt' INTO TABLE tbl_name COLUMNS TERMINATED BY 0x01 ENCLOSED BY '\"' LINES STARTING BY 0x002 TERMINATED BY 0x003 IGNORE 1 LINES;",
                            text);
    }

    public void test_5() throws Exception {
        String sql = "LOAD DATA LOCAL INFILE 'include/38_loaddata1.data' ignore into table t38loaddata fields terminated by ',' LINES STARTING BY ',' (b,c,d) ";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA LOCAL INFILE 'include/38_loaddata1.data' IGNORE  INTO TABLE t38loaddata COLUMNS TERMINATED BY ',' LINES STARTING BY ',' (b, c, d)",
                            text);
    }

    public void test_6() throws Exception {
        String sql = "LOAD DATA LOCAL INFILE 'include/38_loaddata1.data' ignore into table t38loaddata fields terminated by ',' LINES STARTING BY 0x01 (b,c,d) ";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA LOCAL INFILE 'include/38_loaddata1.data' IGNORE  INTO TABLE t38loaddata COLUMNS TERMINATED BY ',' LINES STARTING BY 0x01 (b, c, d)",
                            text);
    }

    public void test_7() throws Exception {
        String sql = "LOAD DATA LOCAL INFILE 'include/38_loaddata1.data' ignore into table t38loaddata fields terminated by ',' LINES STARTING BY x'001' (b,c,d) ";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("LOAD DATA LOCAL INFILE 'include/38_loaddata1.data' IGNORE  INTO TABLE t38loaddata COLUMNS TERMINATED BY ',' LINES STARTING BY 0x001 (b, c, d)",
                            text);
    }

    private String output(List<SQLStatement> stmtList) {
        return SQLUtils.toSQLString(stmtList, JdbcConstants.MYSQL);
    }
}
