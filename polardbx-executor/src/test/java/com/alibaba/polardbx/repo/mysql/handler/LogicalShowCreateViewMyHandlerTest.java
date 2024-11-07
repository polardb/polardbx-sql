package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateView;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LogicalShowCreateViewMyHandlerTest {

    @Test
    public void testFetchSchemaName() {
        LogicalShowCreateViewMyHandler logicalShowCreateViewMyHandler =
            spy(new LogicalShowCreateViewMyHandler(mock(IRepository.class)));

        String schemaName = "INFORMATION_SCHEMA";
        String testTableName = "test_table";
        //mock input
        LogicalShow logicalPlan = mock(LogicalShow.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);

        //mock process
        SqlShowCreateView sqlShowCreateView = mock(SqlShowCreateView.class);
        when(logicalPlan.getNativeSqlNode()).thenReturn(sqlShowCreateView);
        SqlNode tableName = mock(SqlNode.class);
        when(tableName.toString()).thenReturn(testTableName);

        when(sqlShowCreateView.getTableName()).thenReturn(tableName);
        when(logicalPlan.getSchemaName()).thenReturn(schemaName);

        InformationSchemaViewManager viewManager = mock(InformationSchemaViewManager.class);
        SystemTableView.Row row = mock(SystemTableView.Row.class);
        when(viewManager.select(any())).thenReturn(row);

        try (MockedStatic<InformationSchemaViewManager> mockedStatic = mockStatic(InformationSchemaViewManager.class)) {
            mockedStatic.when(InformationSchemaViewManager::getInstance).thenReturn(viewManager);
            //call, once the result of handle() not be null, that means we fetch schemaName from logicalShow successfully
            Assert.assertTrue(logicalShowCreateViewMyHandler.handle(logicalPlan, executionContext) != null);
        }
    }
}
