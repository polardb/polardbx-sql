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

package com.alibaba.polardbx.qatest.ddl.sharding.fastchecker;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Set;
/**
 * Created by zhuqiwei.
 *
 * @author: zhuqiwei
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
@NotThreadSafe
public class FastCheckerWithGsiTest extends FastCheckerTestBase {
    private static String createGsiTemplate =
            "CREATE GLOBAL INDEX {0} ON {1} ({2}) dbpartition by hash({3});";
    private static String checkGsi =
            "/*+TDDL: CMD_EXTRA(CHECK_GLOBAL_INDEX_USE_FASTCHECKER={0})*/ CHECK GLOBAL INDEX {1};";
    private static String deleteItem =
            "delete from {0} where id in (select id from (select id from {0} limit 2) as temp)";
    private static String lockTables =
            "lock tables {0}";
    private static String unlockTables =
            "unlock tables";

    @Before
    public void prepareData() {
        String sql = "select database(),@@sql_mode";
        PreparedStatement stmt = JdbcUtil.preparedStatement(sql, tddlConnection);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            if (rs.next()) {
                schemaName = (String) rs.getObject(1);
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + quoteSpecialName(srcLogicTable));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(srcTableTemplate, srcLogicTable, srcTbPartition));

        for (int i = 1; i <= 100; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                    MessageFormat.format(fullColumnInsert, srcLogicTable, i, "uuid()", "uuid()", "FLOOR(Rand() * 1000)",
                            "now()"));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(createGsiTemplate, quoteSpecialName("g_i_cname"), srcLogicTable, "c_name", "c_name"));
    }

    @After
    public void cleanData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + quoteSpecialName(srcLogicTable));
    }

    @Test
    public void test01_checkGsiWithoutFastChecker() {
        String tddlSql = MessageFormat.format(checkGsi, "false", quoteSpecialName("g_i_cname"));
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            rs = stmt.executeQuery();
            boolean hasSummary = false;
            while (rs.next()) {
                String errorType = rs.getString("ERROR_TYPE");
                if (errorType == null || !errorType.equals("SUMMARY")) {
                    continue;
                }
                hasSummary = true;
                String details = rs.getString("DETAILS");
                if (details == null || !details.contains("OK") || !details.contains("rows checked")) {
                    Assert.fail("GSI check message not found");
                }
                break;
            }
            if (hasSummary == false) {
                Assert.fail("GSI check message not found");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    @Test
    public void test02_checkGsiWithFastCheckerSucceed() {
        String tddlSql = MessageFormat.format(checkGsi, "true", quoteSpecialName("g_i_cname"));
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            rs = stmt.executeQuery();
            boolean hasSummary = false;
            while (rs.next()) {
                String errorType = rs.getString("ERROR_TYPE");
                if (errorType == null || !errorType.equals("SUMMARY")) {
                    continue;
                }
                hasSummary = true;
                String details = rs.getString("DETAILS");
                if (details == null || !details.contains("FastChecker check OK")) {
                    Assert.fail("Fastchecker message not found");
                }
                break;
            }
            if (hasSummary == false) {
                Assert.fail("Fastchecker message not found");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    @Test
    public void test03_checkGsiWithFastCheckerFailed() {
        makeChaos();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String tddlSql = MessageFormat.format(checkGsi, "true", "g_i_cname");
        try {
            stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            rs = stmt.executeQuery();
            boolean hasSummary = false;
            while (rs.next()) {
                String errorType = rs.getString("ERROR_TYPE");
                if (errorType == null || !errorType.equals("SUMMARY")) {
                    continue;
                }
                hasSummary = true;
                String details = rs.getString("DETAILS");
                if (details == null || details.contains("FastChecker check OK") || !details.contains("error found")) {
                    Assert.fail("error not be found");
                }
                break;
            }
            if (hasSummary == false) {
                Assert.fail("error not be found");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    /**
     * this test is time consuming because we must wait until timeout and retry n times.
     */
/*    @Test
    public void test04_checkGsiWithFastCheckerLockTimeout() {
        makeChaos();
        //lock table to make timeout
        Map<String, Set<String>> groupAndTables = getTableTopology(srcLogicTable);
        String group = groupAndTables.keySet().stream().findFirst().get();
        String phyTable = groupAndTables.get(group).stream().findAny().get();
        Connection conn = TddlDataSourceUtil.getPolardbXShardDbConnectionByGroupName(schemaName, group);
        if(conn == null){
            Assert.fail("failed to get phy connection");
        }
        String phySql = MessageFormat.format(lockTables, phyTable + " write");
        Statement stmt = null;
        boolean lockSucceed = true;
        try{
            stmt = conn.createStatement();
            stmt.execute(phySql);
        }catch (SQLException e) {
            lockSucceed = false;
            Assert.fail("语句并未按照预期执行成功:" + e.getMessage());
        }finally {
            if(lockSucceed == false){
                try{
                    if(stmt != null) {
                        stmt.close();
                    }
                    if(conn != null) {
                        conn.close();
                    }
                }catch(SQLException e){}
            }
        }

        //execute Gsi check
        String tddlSql = MessageFormat.format(checkGsi, "true", "g_i_cname");
        try{
            JdbcUtil.executeQueryFaied(tddlConnection, tddlSql, "gsi fastchecker retry exceed max times");
        }finally {
            try{
                stmt = conn.createStatement();
                stmt.execute(unlockTables);
            }catch (Throwable e){}
            finally {
                try{
                    if(stmt != null) {
                        stmt.close();
                    }
                    if(conn != null) {
                        conn.close();
                    }
                }catch(SQLException e){}
            }
        }
    }*/
    private void makeChaos() {
        Map<String, Set<String>> groupAndTables = getTableTopology(srcLogicTable);
        String group = groupAndTables.keySet().stream().findFirst().get();
        String phyTable = groupAndTables.get(group).stream().findAny().get();

        Connection conn = getMySQLPhysicalConnectionByGroupName(schemaName, group);
        if (conn == null) {
            Assert.fail("failed to get phy connection");
        }
        String phySql = MessageFormat.format(deleteItem, phyTable);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(phySql);
        } catch (SQLException e) {
            Assert.fail("语句并未按照预期执行成功:" + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

}

