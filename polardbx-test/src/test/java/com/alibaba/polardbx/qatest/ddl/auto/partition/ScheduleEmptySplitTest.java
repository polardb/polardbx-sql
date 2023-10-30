package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public class ScheduleEmptySplitTest extends PartitionTestBase {
    private static final String TABLE_NAME_1 = "test_schedule_empty_split";

    private static final String TABLE_NAME_2 = "test_schedule_empty_split_broadcast";

    private static final String CREATE_TABLE = "create table %s (c1 char(255), c2 char(255) )";
    private static final String DROP_TABLE = "drop table if exists %s";

    @Before
    public void prepare() {
        dropTable();
        createTable();
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME_2));
    }

    private void createTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE + " partition by hash(c1)", TABLE_NAME_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE + " broadcast", TABLE_NAME_2));
    }

    @Test
    public void testScheduleEmptySplit() throws SQLException {
        String sql =
            String.format(
                "/*+TDDL: workload_type=ap EXECUTOR_MODE=AP_LOCAL enable_push_agg=false enable_cbo_push_agg=false*/ select tx.c2, count(distinct tx.c1) from %s tx join %s txx on tx.c1 = txx.c1 where 1 = 2 group by tx.c2",
                TABLE_NAME_1, TABLE_NAME_2);
        JdbcUtil.executeSuccess(tddlConnection, "begin");
        JdbcUtil.executeSuccess(tddlConnection, "set share_read_view=false");
        JdbcUtil.executeSuccess(tddlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, "explain physical " + sql);
        JdbcUtil.executeSuccess(tddlConnection, "commit");
    }
}
