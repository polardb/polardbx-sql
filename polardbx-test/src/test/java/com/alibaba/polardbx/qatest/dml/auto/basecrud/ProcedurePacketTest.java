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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.mysql.jdbc.JDBC42ResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ProcedurePacketTest extends BaseTestCase {
    protected Connection tddlConnection;

    private String procedure1 = "create procedure procedure1() \n"
        + "begin\n"
        + "declare x int default 1;\n"
        + "select 1 + x;\n"
        + "select '?' + x + \"?\";\n"
        + "select 'te?st?' + x;\n"
        + "end";

    private String procedure2 = "create procedure procedure2() \n"
        + "begin\n"
        + "declare x int default 1;\n"
        + "select 1 + x;\n"
        + "select 'te?st?' + x;\n"
        + "select '?' + `?` + x + \"?\";\n"
        + "select 'te?st?' + x;\n"
        + "end";

    @Before
    public void getConnection() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        dropProcedure();
    }

    @Test
    public void executeFailed() throws SQLException {
        JdbcUtils.execute(tddlConnection, procedure2);
        ResultSet rs = null;
        Statement statement = null;
        int oldId = -1;
        int newId = -2;
        try {
            statement = tddlConnection.createStatement();
            rs = statement.executeQuery("select connection_id();");
            if (rs.next()) {
                oldId = rs.getInt(1);
            }
            statement.executeQuery("call procedure2");
        } catch (Exception ex) {
            rs = statement.executeQuery("select connection_id();");
            if (rs.next()) {
                newId = rs.getInt(1);
            }
            Assert.assertTrue(oldId == newId, "connection was reset unexpected!");
        } finally {
            statement.close();
            rs.close();
        }
    }

    @Test
    public void normalExecute() throws SQLException {
        JdbcUtils.execute(tddlConnection, procedure1);
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery("call procedure1")) {
            if (!rs.next()) {
                Assert.fail("no result found!");
            }
            if (rs.getInt(1) != 2) {
                Assert.fail("select result not matched!");
            }
            ResultSet child1 = checkChildRs(rs);
            checkChildRs(child1);
        }
    }

    @After
    public void dropProcedure() throws SQLException {
        JdbcUtils.execute(tddlConnection, "drop procedure if exists procedure1");
        JdbcUtils.execute(tddlConnection, "drop procedure if exists procedure2");
    }

    private ResultSet checkChildRs(ResultSet rs) throws SQLException {
        if (!(rs instanceof JDBC42ResultSet)) {
            Assert.fail("expect jdbc result set");
        }
        ResultSet childRs = ((JDBC42ResultSet) rs).getNextResultSet();
        if (!childRs.next()) {
            Assert.fail("no result found");
        }
        if (childRs.getInt(1) != 1) {
            Assert.fail("select result not matched!");
        }
        return childRs;
    }
}
