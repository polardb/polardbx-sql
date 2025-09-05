package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class BreakpointResumeTest extends DDLBaseNewDBTestCase {
    private static final String TABLE_NAME = "t_breakpoint_resume";
    private final String partitionBy;
    private final Long threadPoolSize;

    public BreakpointResumeTest(String partitionBy, Long threadPoolSize) {
        this.partitionBy = partitionBy;
        this.threadPoolSize = threadPoolSize;
    }

    @Parameterized.Parameters(name = "{index}:useInstantAddColumn={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {"single", 4L}, new Object[] {"broadcast", 2L},
            new Object[] {"partition by key(b) partitions 10", 1L});
    }

    private void prepareTable() {
        String createTable =
            String.format("create table %s(a varchar(60) primary key, b int) %s", TABLE_NAME, partitionBy);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        StringBuilder sb = new StringBuilder(String.format("insert into %s values", TABLE_NAME));
        for (int i = 0; i < 1024; i++) {
            sb.append(String.format("(UUID(), %s)", i));
            if (i != 1023) {
                sb.append(",");
            }
        }
        String insertSql = sb.toString();

        // load data
        for (int i = 0; i < 1000; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + TABLE_NAME);
    }

    private void clean() {
        String dropTable = String.format("drop table if exists %s", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    @Test
    public void testBreakpointResume() throws SQLException {
        clean();
        prepareTable();
        JdbcUtil.executeSuccess(tddlConnection, "set global OMC_THREAD_POOL_SIZE = " + threadPoolSize);
        System.out.println("prepareData success");

        String hint =
            "/*+TDDL:cmd_extra(PHYSICAL_TABLE_BACKFILL_PARALLELISM=8,PHYSICAL_TABLE_START_SPLIT_SIZE = 10, ENABLE_INSERT_IGNORE_FOR_OMC=true)*/";
        String sql = hint + String.format("alter table %s modify column a varchar(100), algorithm = omc, async=true",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        JobInfo ddlJobInfo = fetchCurrentJob(TABLE_NAME);
        if (ddlJobInfo == null) {
            return;
        }

        long jobId = ddlJobInfo.parentJob.jobId;
        String pauseSql = String.format("pause ddl %s", jobId);
        String continueSql = String.format("continue ddl %s async=true", jobId);
        String continueSql2 = String.format("continue ddl %s", jobId);

        int loopCount = 0;
        while ((ddlJobInfo = fetchCurrentJob(TABLE_NAME)) == null
            || ddlJobInfo.parentJob.backfillProgress.contains("-")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 100) {
                System.out.println("waiting start backfill failed");
                return;
            }
        }

        for (int i = 0; i < 10; i++) {
            ddlJobInfo = fetchCurrentJob(TABLE_NAME);
            if (ddlJobInfo == null) {
                break;
            }

            try (Statement statement = JdbcUtil.createStatement(tddlConnection)) {
                statement.execute(pauseSql);
                System.out.printf("pause ddl times %s success\n", i + 1);
            } catch (SQLException e) {
                if (e.getMessage().contains("The ddl job does not exist")) {
                    return;
                }
                System.out.println("pause ddl failed");
                throw e;
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            String continueDdl = i == 9 ? continueSql2 : continueSql;
            try (Statement statement = JdbcUtil.createStatement(tddlConnection)) {
                statement.execute(continueDdl);
                System.out.printf("continue ddl times %s success\n", i + 1);
            } catch (SQLException e) {
                if (e.getMessage().contains("The ddl job does not exist")) {
                    return;
                }
                System.out.println("continue ddl failed");
                throw e;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        ddlJobInfo = fetchCurrentJob(TABLE_NAME);
        Assert.assertTrue(ddlJobInfo == null);

        checkDdlJobBackfillObjects(jobId);
        System.out.println("check backfill objects success");

        clean();
        System.out.println("clean success");
    }

    private void checkDdlJobBackfillObjects(Long jobId) {
        String sql = String.format(
            "select last_value = max_value from metadb.backfill_objects "
                + "where job_id in "
                + "(select task_id from metadb.ddl_engine_task_archive where job_id = %s and name = 'LogicalTableBackFillTask') "
                + "and physical_db is not null and physical_table is not null", jobId);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Assert.assertTrue(rs.getInt(1) == 1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}