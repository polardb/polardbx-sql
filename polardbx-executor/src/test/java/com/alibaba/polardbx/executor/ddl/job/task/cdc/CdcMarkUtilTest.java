package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import org.junit.Assert;
import org.junit.Test;

public class CdcMarkUtilTest {
    @Test
    public void testScheduleJobDdlRegex() {
        Assert.assertFalse(CdcMarkUtil.isScheduleJobDdl("alter table t1 add column c1 int"));
        Assert.assertTrue(CdcMarkUtil.isScheduleJobDdl("/*SCHEDULE_JOB_DDL*/alter table t1 add column c1 int"));
        
    }
}
