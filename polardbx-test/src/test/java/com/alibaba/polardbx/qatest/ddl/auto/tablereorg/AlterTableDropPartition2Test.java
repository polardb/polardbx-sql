package com.alibaba.polardbx.qatest.ddl.auto.tablereorg;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.showFullCreateTable;
import static org.hamcrest.Matchers.is;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableDropPartition2Test extends DDLBaseNewDBTestCase {

    private static final String tableSchema = "_dropPartitionDb_";
    private static String CREATE_TABLE_PATTERN1 = "create table if not exists %s (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ) (\n"
        + "subpartition p1sp1 values in ( 1000, 2000),\n"
        + "subpartition p1sp2 values in ( default )\n"
        + "),\n"
        + "partition p2 values less than ( maxvalue ) (\n"
        + "subpartition p2sp1 values in ( 1000, 2000),\n"
        + "subpartition p2sp2 values in ( 3000, 4000),\n"
        + "subpartition p2sp3 values in ( default )\n"
        + ")\n"
        + ");";
    private static String CREATE_TABLE_PATTERN2 = "create table if not exists %s (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000),\n"
        + "subpartition sp2 values less than ( maxvalue )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01') ),\n"
        + "partition p2 values in ( default )\n"
        + ");";

    @Test
    public void ntpSubpartitionTest() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String dropTable = "drop table if exists %s";
        String tb1 = "tb" + RandomUtils.getStringBetween(5, 8);
        String sql = String.format(dropTable, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN1, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "/*+TDDL:CMD_EXTRA(TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='WRITE_REORG')*/alter table " + tb1
            + " drop subpartition p1sp1";
        String ignoreErr =
            "The DDL job has been paused or cancelled. Please use SHOW DDL";
        Set<String> ignoreErrs = new HashSet<>();
        ignoreErrs.add(ignoreErr);
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sql,
            ignoreErrs);

        sql = "trace select * from " + tb1;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.toString(), trace.size(), is(4));

        sql = "insert into " + tb1 + " values (1000,1000,'2019-01-01','a','b')";
        JdbcUtil.executeFailed(tddlConnection, sql, "Table has no partition for the values");
    }

    @Test
    public void listTableTest() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String dropTable = "drop table if exists %s";
        String tb1 = "tb" + RandomUtils.getStringBetween(5, 8);
        String sql = String.format(dropTable, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN2, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "/*+TDDL:CMD_EXTRA(TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='WRITE_REORG')*/alter table " + tb1
            + " drop subpartition sp1";
        String ignoreErr =
            "The DDL job has been paused or cancelled. Please use SHOW DDL";
        Set<String> ignoreErrs = new HashSet<>();
        ignoreErrs.add(ignoreErr);
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sql,
            ignoreErrs);

        sql = "trace select * from " + tb1;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.toString(), trace.size(), is(2));

        sql = "insert into " + tb1 + " values (1,2,'2019-01-01','a','b')";
        JdbcUtil.executeFailed(tddlConnection, sql, "Table has no partition for the values");
    }

    @Before
    public void setUp() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use polardbx");
        String sql1 = String.format("drop database if exists %s", tableSchema);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = String.format("create database %s mode=auto", tableSchema);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @After
    public void cleanUp() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use polardbx");
        String sql1 = String.format("drop database if exists %s", tableSchema);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }
}
