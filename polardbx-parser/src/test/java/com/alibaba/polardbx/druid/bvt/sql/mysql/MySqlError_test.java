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


import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Assert;

public class MySqlError_test extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "insert into userdetectitem26 (nick,volume) values(?,?) " + //
                     "ON DUPLICATE KEY UPDATE title = ?,picURL = ?,scoreExceed = ?,score=";

        Exception error = null;

        try {
            MySqlStatementParser parser = new MySqlStatementParser(sql);
            parser.parseStatementList();
        } catch (Exception e) {
            error = e;
        }
        
        Assert.assertNotNull(error);
        Assert.assertEquals("EOF, score=", error.getMessage());
    }

    public void test_1() throws Exception {
        String sql = "delete from move_trade_trust where id=3303944334 and userid={userid}";

        Exception error = null;

        try {
            MySqlStatementParser parser = new MySqlStatementParser(sql);
            parser.parseStatementList();
        } catch (Exception e) {
            error = e;
        }

        Assert.assertNotNull(error);
        Assert.assertEquals("ERROR. pos 67, line 1, column 62, token IDENTIFIER userid", error.getMessage());
    }
}
