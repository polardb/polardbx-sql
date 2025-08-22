package com.alibaba.polardbx.qatest.ddl.ddlProgress;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class DdlProgressTest extends DDLBaseNewDBTestCase {
    private void prepareData(String tableName, int partitions) {
        dropTableIfExists(tableName);
        String createTable =
            String.format("create table %s(a varchar(60) primary key, b int) partition by key(a) partitions %s",
                tableName, partitions);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        StringBuilder sb = new StringBuilder(String.format("insert into %s values", tableName));
        for (int i = 0; i < 1024; i++) {
            sb.append(String.format("(UUID(), %s)", i));
            if (i != 1023) {
                sb.append(",");
            }
        }

        String insertSql = sb.toString();
        // load data
        for (int i = 0; i < 500; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
    }

    @Test
    public void testLogicalDdlProgress() throws SQLException {
        String tableName = "t_ddl_progress_1";
        prepareData(tableName, 3);
        System.out.println("prepareData success");

        String sql = String.format("alter table %s modify column b bigint, algorithm = omc, async=true", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        JobInfo jobInfo = fetchCurrentJob(tableName);

        // 验证 ddl 进度的几个阶段
        // before backfill
        int loopCount = 0;
        while (jobInfo == null || jobInfo.parentJob.backfillProgress.equalsIgnoreCase("--")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 100) {
                System.out.println("waiting for backfill start failed");
                return;
            }
            jobInfo = fetchCurrentJob(tableName);
        }

        // backfill
        System.out.println("start backfill");
        jobInfo = fetchCurrentJob(tableName);
        Assert.assertTrue(jobInfo.parentJob.backfillProgress.contains("%"));

        // show ddl status
        List<String> metricList =
            JdbcUtil.executeQueryAndGetColumnResult("show ddl status", tddlConnection, 1);
        Assert.assertTrue(metricList.contains("OMC_THREAD_POOL_SIZE"));
        Assert.assertTrue(metricList.contains("OMC_THREAD_POOL_NUM"));

        while (jobInfo != null && jobInfo.parentJob.backfillProgress.contains("%")) {
            // check progress
            String progress = jobInfo.parentJob.backfillProgress.replace("%", "");
            System.out.println("backfill progress " + jobInfo.parentJob.backfillProgress);
            Assert.assertTrue(Long.parseLong(progress) <= 100);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 600) {
                System.out.println("waiting for ddl finished failed");
                return;
            }
            jobInfo = fetchCurrentJob(tableName);
        }
        System.out.println("backfill end");

        System.out.println("waiting finished");
        loopCount = 0;
        while (jobInfo != null && jobInfo.parentJob.state.equalsIgnoreCase("RUNNING")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 600) {
                System.out.println("waiting for ddl finished failed");
                return;
            }
            jobInfo = fetchCurrentJob(tableName);
        }
        System.out.println("ddl finished");
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
