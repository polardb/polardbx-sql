package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;

public class CciSnapshotCheckerTest {
    @Test
    public void test0() {
        ExecutionContext context = new ExecutionContext();
        context.getParamManager().getProps().put("MPP_PARALLELISM", "1024");
        context.getParamManager().getProps().put("PARALLELISM", "1024");
        context.getParamManager().getProps().put("ENABLE_MPP", "true");
        context.getParamManager().getProps().put("ENABLE_MASTER_MPP", "true");
        CciSnapshotChecker checker = new CciSnapshotChecker("test_schema",
            "test_table", "test_index", 100L, 200L);
        Assert.assertEquals(Long.valueOf(100L), checker.getCheckTso().getKey());
        Assert.assertEquals(Long.valueOf(200L), checker.getCheckTso().getValue());
        String columnarSql = checker.getColumnarSql(context, 100L, "id, a");
        Assert.assertTrue(columnarSql.contains("MPP_PARALLELISM=1024"));
        Assert.assertTrue(columnarSql.contains("PARALLELISM=1024"));
        Assert.assertTrue(columnarSql.contains("ENABLE_MPP=true"));
        Assert.assertTrue(columnarSql.contains("ENABLE_MASTER_MPP=true"));
        Assert.assertTrue(columnarSql.contains("check_sum_v2(id, a) as pk_checksum"));
        Assert.assertTrue(columnarSql.contains("force index(test_index)"));
        String primarySql = checker.getPrimarySql(context, 100L, "id, a");
        Assert.assertTrue(primarySql.contains("MPP_PARALLELISM=1024"));
        Assert.assertTrue(primarySql.contains("PARALLELISM=1024"));
        Assert.assertTrue(primarySql.contains("ENABLE_MPP=true"));
        Assert.assertTrue(primarySql.contains("ENABLE_MASTER_MPP=true"));
        Assert.assertTrue(primarySql.contains("check_sum_v2(id, a) as pk_checksum"));
        Assert.assertTrue(primarySql.contains("force index(primary)"));
    }

    @Test
    public void test1() {
        ExecutionContext context = new ExecutionContext();
        context.getParamManager().getProps().put("MPP_PARALLELISM", "1024");
        context.getParamManager().getProps().put("PARALLELISM", "1024");
        context.getParamManager().getProps().put("ENABLE_MPP", "true");
        context.getParamManager().getProps().put("ENABLE_MASTER_MPP", "true");
        CciSnapshotFastChecker checker = new CciSnapshotFastChecker("test_schema",
            "test_table", "test_index", 100L, 200L);
        Assert.assertEquals(Long.valueOf(100L), checker.getCheckTso().getKey());
        Assert.assertEquals(Long.valueOf(200L), checker.getCheckTso().getValue());
        String primarySql = checker.getPrimarySql(context, 100L);
        Assert.assertTrue(primarySql.contains("MPP_PARALLELISM=1024"));
        Assert.assertTrue(primarySql.contains("PARALLELISM=1024"));
        Assert.assertTrue(primarySql.contains("ENABLE_MPP=true"));
        Assert.assertTrue(primarySql.contains("ENABLE_MASTER_MPP=true"));
        Assert.assertTrue(primarySql.contains("force index(primary)"));
    }
}
