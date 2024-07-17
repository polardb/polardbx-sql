package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TraceDDLTest extends DDLBaseNewDBTestCase {

    @Test
    public void testTraceCreateTable() throws SQLException {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "trace /*+TDDL:cmd_extra(SKIP_DDL_RESPONSE = true)*/create table t1(a varchar(100))";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "show trace";
        ResultSet rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        rs.close();
    }

    public boolean usingNewPartDb() {
        return true;
    }
}
