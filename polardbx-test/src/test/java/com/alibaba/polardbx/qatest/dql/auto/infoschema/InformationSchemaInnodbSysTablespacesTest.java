package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertWithMessage;

public class InformationSchemaInnodbSysTablespacesTest extends DDLBaseNewDBTestCase {
    private static final String INFO_SPACES =
        "select name from information_schema.innodb_sys_tablespaces where name='%s'";

    @Test
    public void testTableInnodbSysTablespaces() throws SQLException {
        if (isMySQL80()) {
            return;
        }
        String db = tddlDatabase1;
        String tb = "test_sys_tablespaces";

        String sql = String.format("create table `%s` (a int, b int)", tb);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        String name = String.format("%s/%s", db, tb);
        sql = String.format(INFO_SPACES, name);
        checkInfoSpaces(name, sql, tddlConnection);
    }

    @Test
    public void testTableInnodbSysTablespaces2() throws SQLException {
        if (isMySQL80()) {
            return;
        }
        String db = tddlDatabase1;
        String tb = "test-sys$tablespaces";

        String sql = String.format("create table `%s` (a int, b int)", tb);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        String name = String.format("%s/%s", db, tb);
        sql = String.format(INFO_SPACES, name);
        checkInfoSpaces(name, sql, tddlConnection);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private void checkInfoSpaces(String name, String sql, Connection connection) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        boolean found =
            JdbcUtil.getAllResult(rs).stream().anyMatch(x -> name.equalsIgnoreCase((String) x.get(0)));
        assertWithMessage("find table " + name + " in 'information_schema.innodb_sys_tablespaces'")
            .that(found).isTrue();
        rs.close();
    }
}
