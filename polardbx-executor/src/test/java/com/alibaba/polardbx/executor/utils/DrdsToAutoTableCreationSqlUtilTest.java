package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.executor.TopologyExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.repo.RepositoryHolder;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowTablesMyHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowTables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DrdsToAutoTableCreationSqlUtilTest {

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private LogicalShowTablesMyHandler sourceRepo;

    @InjectMocks
    private DrdsToAutoTableCreationSqlUtil util;

    @Before
    public void setUp() {
        when(executionContext.getSchemaName()).thenReturn("testSchema");
    }

    @Test
    public void testGetTableNamesFromDatabase() {
        // Prepare mock data
        String[] tableData = {"table1", "table2"};
        ArrayResultCursor resultCursor = new ArrayResultCursor("TABLES");

        resultCursor.addColumn("Tables_in_testschema", DataTypes.StringType, false);
        resultCursor.addColumn("Table_type", DataTypes.StringType, false);
        resultCursor.addColumn("Auto_partition", DataTypes.StringType, false);
        resultCursor.addRow(new Object[] {tableData[0], "BASE TABLE", "YES"});
        resultCursor.addRow(new Object[] {tableData[1], "BASE TABLE", "YES"});
        resultCursor.addRow(new Object[] {"testview", "view", "YES"});

        AsyncDDLContext asyncDDLContext = mock(AsyncDDLContext.class);
        Planner planner = mock(Planner.class);
        ExecutionPlan showTablesPlan = mock(ExecutionPlan.class);
        LogicalShow logicalShowTables = mock(LogicalShow.class);
        ExecutorContext executorContext = mock(ExecutorContext.class);
        TopologyHandler topologyHandler = mock(TopologyHandler.class);
        RepositoryHolder repositoryHolder = mock(RepositoryHolder.class);
        IRepository iRepository = mock(IRepository.class);

        when(executionContext.copy()).thenReturn(executionContext);
        when(executionContext.getAsyncDDLContext()).thenReturn(asyncDDLContext);
        when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);
        when(topologyHandler.getRepositoryHolder()).thenReturn(repositoryHolder);
        when(repositoryHolder.get(anyString())).thenReturn(iRepository);

        try (MockedStatic<Planner> mockedPlanner = Mockito.mockStatic(Planner.class);
            MockedStatic<ExecutorContext> mockedExecutorContext = Mockito.mockStatic(ExecutorContext.class);
            MockedConstruction<LogicalShowTablesMyHandler> mocked = Mockito.mockConstruction(
                LogicalShowTablesMyHandler.class,
                (mock, context) -> {
                    Mockito.when(mock.handle(any(RelNode.class), any(ExecutionContext.class))).thenReturn(resultCursor);
                });) {

            mockedPlanner.when(() -> Planner.getInstance()).thenReturn(planner);
            when(planner.getPlan(any(SqlShowTables.class), any(PlannerContext.class))).thenReturn(showTablesPlan);
            when(showTablesPlan.getPlan()).thenReturn(logicalShowTables);

            mockedExecutorContext.when(() -> ExecutorContext.getContext(anyString())).thenReturn(executorContext);

            // Execute method under test
            List<String> result = util.getTableNamesFromDatabase("testSchema", executionContext);

            // Assert the result
            assertEquals(Arrays.asList(tableData), result);
        }
    }
}
