package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
public class CdcAuthMarkTest extends CdcBaseTest {
    protected final String polardbxPort = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT);
    protected final String SERVER_1 = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);
    String dbName = "cdc_auth_test";

    @Test
    public void testCdcDdlRecord() throws SQLException {
        String user1 = "cdc_user_" + RandomUtils.nextLong();
        String user2 = "cdc_user_" + RandomUtils.nextLong();
        String role1 = "cdc_role_" + RandomUtils.nextLong();
        String role2 = "cdc_role_" + RandomUtils.nextLong();

        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "create database " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "use " + dbName);

        try (Statement stmt = tddlConnection.createStatement()) {
            executeAndCheck(stmt, "CREATE USER '" + user1 + "'@'127.0.0.1' IDENTIFIED BY '123456'");
            executeAndCheck(stmt, "CREATE USER IF NOT EXISTS '" + user2 + "'@'%' identified by '123456'");
            executeAndCheck(stmt, "SET PASSWORD FOR '" + user2 + "'@'%' = PASSWORD('654321')");
            executeAndCheck(stmt, String.format(
                "GRANT SELECT,UPDATE ON `%s`.* TO '%s'@'127.0.0.1'", tddlConnection.getSchema(), user1));
            executeAndCheck(stmt, String.format(
                "REVOKE UPDATE ON %s.* FROM '%s'@'127.0.0.1'", tddlConnection.getSchema(), user2
            ));

            executeAndCheck(stmt, "CREATE ROLE '" + role1 + "'@'%', '" + role2 + "'");
            executeAndCheck(stmt, "GRANT ALL PRIVILEGES ON " + tddlConnection.getSchema() + ".* TO '" + role1 + "'");
            executeAndCheck(stmt, "GRANT ALL PRIVILEGES ON " + tddlConnection.getSchema() + ".* TO '" + role2 + "'");
            executeAndCheck(stmt, "GRANT '" + role1 + "' TO '" + user1 + "'@'127.0.0.1'");
            executeAndCheck(stmt, "GRANT '" + role1 + "' TO '" + user2 + "'@'%'");
            executeAndCheck(stmt, "GRANT '" + role2 + "' TO '" + user1 + "'@'127.0.0.1'");
            executeAndCheck(stmt, "GRANT '" + role2 + "' TO '" + user2 + "'@'%'");
            executeAndCheck(stmt, "SET DEFAULT ROLE '" + role1 + "' TO '" + user2 + "'@'%'");
            executeAndCheck(stmt, "REVOKE ALL PRIVILEGES ON " + tddlConnection.getSchema() + ".* FROM '" + role1 + "'");
            executeAndCheck(stmt, "REVOKE ALL PRIVILEGES ON " + tddlConnection.getSchema() + ".* FROM '" + role2 + "'");
            executeAndCheck(stmt, "REVOKE '" + role1 + "' FROM '" + user1 + "'@'127.0.0.1'");
            executeAndCheck(stmt, "REVOKE '" + role1 + "' FROM '" + user2 + "'@'%'");

            executeAndCheck(stmt, "SET PASSWORD FOR " + user1 + "@'127.0.0.1' = '123456'");
            checkSetPasswordNotMarkDdl(user2);

            executeAndCheck(stmt, "DROP USER '" + user1 + "'@'127.0.0.1'");
            executeAndCheck(stmt, "DROP USER '" + user2 + "'@'%'");
            executeAndCheck(stmt, "DROP ROLE '" + role1 + "'@'%'");
            executeAndCheck(stmt, "DROP ROLE '" + role2 + "'@'%'");
        }

    }

    private void executeAndCheck(Statement stmt, String ddl) throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        List<DdlRecordInfo> list = getDdlRecordInfoListByToken(tokenHints);

        Assert.assertEquals(1, list.size());
        assertSqlEquals(sql, list.get(0).getEffectiveSql());
        Assert.assertEquals("polardbx", list.get(0).getSchemaName());
        Assert.assertEquals("*", list.get(0).getTableName());
        Assert.assertEquals(DdlScope.Instance.getValue(), list.get(0).getDdlExtInfo().getDdlScope());
    }

    private void checkSetPasswordNotMarkDdl(String user) throws SQLException {
        DdlCheckContext context = newDdlCheckContext();
        int before = context.getMarkList(dbName).size();

        Connection connection = getPolardbxDirectConnection(SERVER_1, user, "654321", polardbxPort);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("SET PASSWORD = '123456'");
        }
        int after = context.getMarkList(dbName).size();
        Assert.assertEquals(after, before);
    }
}
