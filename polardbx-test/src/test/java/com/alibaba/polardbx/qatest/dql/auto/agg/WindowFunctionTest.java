package com.alibaba.polardbx.qatest.dql.auto.agg;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;

public class WindowFunctionTest extends AutoReadBaseTestCase {
    private static final String TABLE_NAME = "test_window_function";

    @Before
    public void prepareNormalData() throws Exception {
        JdbcUtil.useDb(tddlConnection, polardbXAutoDBName1());
        dropTableIfExists();

        String createSql =
            "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=false)*/CREATE TABLE if not exists `" + TABLE_NAME + "` (\n"
                + "\t`id1` int(10) DEFAULT NULL,\n"
                + "\t`id2` double,\n"
                + "\t`id3` float,\n"
                + "\t`id4` double,\n"
                + "\t`id5` decimal(10,0),\n"
                + "        KEY `auto_shard_key_id1` USING BTREE (`id1`)\n"
                + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + " partition by hash(`id1`)");

        String insertSql = "insert into " + TABLE_NAME + " values "
            + "(NULL,1,1,1,1),"
            + "(NULL,1,1,1,1),"
            + "(0,1,1,1,1),"
            + "(0,1,1,1,1),"
            + "(0,2,1,1,1),"
            + "(0,2,1,1,1),"
            + "(1,1,1,1,1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
    }

    @Test
    public void testWindowWithAgg() {
        String sql = String.format(
            "select avg(id2) over w1, avg(id3) over w1, avg(id4) over w1, avg(id5) over w1 from %s window w1 as (partition by id1 order by id1 ROWS 2 PRECEDING)",
            TABLE_NAME);
        JdbcUtil.useDb(tddlConnection, polardbXAutoDBName1());
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @After
    public void dropTableIfExists() {
        String dropTable = "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=false)*/drop table if exists " + TABLE_NAME;
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }
}
