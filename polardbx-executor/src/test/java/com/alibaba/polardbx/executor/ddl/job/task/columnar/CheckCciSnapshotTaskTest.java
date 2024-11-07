package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.executor.columnar.checker.ICciChecker;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckCciPrepareData;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCheckColumnarIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckCciSnapshotTaskTest {
    @Test
    public void testFailedWithOneResult() throws Throwable {
        String schema = "test";
        String table = "test";
        String index = "test";
        SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd = SqlCheckColumnarIndex.CheckCciExtraCmd.SNAPSHOT;
        List<Long> tsoList = ImmutableList.of(100L, 200L);
        CheckCciPrepareData checkCciPrepareData = new CheckCciPrepareData(schema, table, index, extraCmd, tsoList);
        CheckCciSnapshotTask task = CheckCciSnapshotTask.create(checkCciPrepareData);
        Assert.assertEquals(schema, task.getSchemaName());
        Assert.assertEquals(table, task.getTableName());
        Assert.assertEquals(index, task.getIndexName());

        ExecutionContext ec = new ExecutionContext();
        ICciChecker checker = mock(ICciChecker.class);
        when(checker.getCheckReports(any())).then(
            invocation -> {
                Collection<String> reports = invocation.getArgument(0);
                reports.add("testFailedWithOneResult");
                return false;
            }
        );

        task.setJobId(0L);
        task.doCheck(ec, checker);
        System.out.println(task.getReports());
        Assert.assertTrue(task.getReports().size() == 2 && task.getReports().get(0).getDetails()
            .contains("testFailedWithOneResult"));
    }

    @Test
    public void testFailWithException() throws Throwable {
        String schema = "test";
        String table = "test";
        String index = "test";
        SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd = SqlCheckColumnarIndex.CheckCciExtraCmd.SNAPSHOT;
        List<Long> tsoList = ImmutableList.of(100L, 200L);
        CheckCciPrepareData checkCciPrepareData = new CheckCciPrepareData(schema, table, index, extraCmd, tsoList);
        CheckCciSnapshotTask task = CheckCciSnapshotTask.create(checkCciPrepareData);
        Assert.assertEquals(schema, task.getSchemaName());
        Assert.assertEquals(table, task.getTableName());
        Assert.assertEquals(index, task.getIndexName());

        ExecutionContext ec = new ExecutionContext();
        ICciChecker checker = mock(ICciChecker.class);
        doThrow(new RuntimeException("testFailWithException")).when(checker).check(ec);

        task.setJobId(0L);
        task.doCheck(ec, checker);
        System.out.println(task.getReports());
        Assert.assertTrue(task.getReports().size() == 2 && task.getReports().get(0).getDetails()
            .contains("Error occurs when checking, caused by testFailWithException"));
    }
}
