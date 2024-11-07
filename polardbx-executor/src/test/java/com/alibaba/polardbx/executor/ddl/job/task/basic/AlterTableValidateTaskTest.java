package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.utils.Assert;
import com.google.common.collect.Lists;
import junit.framework.TestCase;

import java.util.List;

public class AlterTableValidateTaskTest extends TestCase {
    public void testContainsColumnNameIgnoreCase1() throws Throwable {
        List<String> columnNames = Lists.newArrayList("hello", "World");
        Assert.assertTrue(AlterTableValidateTask.containsColumnNameIgnoreCase(columnNames, "HELLO"));
    }

    public void testContainsColumnNameIgnoreCase2() throws Throwable {
        List<String> columnNames = Lists.newArrayList("hello", "World");
        Assert.assertTrue(!AlterTableValidateTask.containsColumnNameIgnoreCase(columnNames, "HE"));
    }

    public void testIndexOfColumnNameIgnoreCase1() throws Throwable {
        List<String> columnNames = Lists.newArrayList("hello", "World");
        Assert.assertTrue(AlterTableValidateTask.indexOfColumnNameIgnoreCase(columnNames, "HELLO") == 0);
    }

    public void testIndexOfColumnNamesIgnoreCase2() throws Throwable {
        List<String> columnNames = Lists.newArrayList("hello", "World");
        Assert.assertTrue(AlterTableValidateTask.indexOfColumnNameIgnoreCase(columnNames, "HE") == -1);
    }
}