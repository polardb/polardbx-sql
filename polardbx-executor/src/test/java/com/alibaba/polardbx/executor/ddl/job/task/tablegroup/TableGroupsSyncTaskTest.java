package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.sql.Connection;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class TableGroupsSyncTaskTest {

    /**
     * 初始化 Mockito 注解，用于模拟对象
     */
    public void setUp() {
    }

    /**
     * TC1: 在回滚事务期间，测试 `duringRollbackTransaction` 是否正确调用了 `syncTableGroup`
     */
    @Test
    public void testDuringRollbackTransaction() {
        TableGroupsSyncTask tableGroupsSyncTask = spy(new TableGroupsSyncTask("d1", ImmutableList.of("group1")));
        doNothing().when(tableGroupsSyncTask).syncTableGroup();
        tableGroupsSyncTask.duringRollbackTransaction(mock(Connection.class), mock(ExecutionContext.class));
    }
}
