package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AlterTableCreateOptionsTest extends DDLBaseNewDBTestCase {

    @Test
    public void testAlterTableCreateOptions() throws SQLException {
        String schemaName = getDdlSchema();
        String table_name = "test_" + RandomUtils.getStringBetween(4, 6);
        String sql1 = "drop table if exists " + table_name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table %s (a int, b varchar(100)) partition by key(a) partitions 2";
        JdbcUtil.executeSuccess(tddlConnection, String.format(sql1, table_name));

        sql1 =
                "alter table %s max_rows=9999";
        JdbcUtil.executeSuccess(tddlConnection, String.format(sql1, table_name));

        sql1 = "select * from information_schema.tables where table_schema='%s' and table_name='%s'";
        ResultSet rs = JdbcUtil.executeQuery(String.format(sql1, schemaName, table_name), tddlConnection);
        if(rs.next()) {
            Assert.assertEquals("max_rows=9999", rs.getString("create_options"));
        }
        rs.close();

    }


    public boolean usingNewPartDb() {
        return true;
    }
}
