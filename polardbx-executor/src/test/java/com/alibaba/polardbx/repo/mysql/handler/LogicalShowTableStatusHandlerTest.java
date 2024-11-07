package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ShowTablesSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ViewsInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.apache.calcite.sql.SqlShowTableStatus;
import org.junit.Test;
import org.mockito.MockedStatic;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LogicalShowTableStatusHandlerTest {
    @Test
    public void testHandle() throws NoSuchFieldException, IllegalAccessException {
        //1. prepare
        LogicalShowTableStatusHandler logicalShowTableStatusHandler =
            spy(new LogicalShowTableStatusHandler(mock(IRepository.class)));

        //doNothing when call showTables
        doNothing().when(logicalShowTableStatusHandler).showTables(any(), any(), any(), any());
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {});
        list.add(new Object[] {});
        doReturn(list).when(
            logicalShowTableStatusHandler).showSingleTableStatusWithColumnarMode(any());

        //make 'repo' in HandleCommon be 'MyRepository'
        Field field = logicalShowTableStatusHandler.getClass().getSuperclass().getSuperclass().getDeclaredField("repo");
        field.setAccessible(true);
        field.set(logicalShowTableStatusHandler, mock(MyRepository.class));

        //set isColumnarMode = true
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(() -> ConfigDataMode.isColumnarMode()).thenReturn(true);
            //2. input parameter
            LogicalShow logicalShow = mock(LogicalShow.class);
            ExecutionContext executionContext = mock(ExecutionContext.class);
            //mock OptimizerContext.getContext and ExecutorContext.getContext
            try (MockedStatic<OptimizerContext> staticOptimizer = mockStatic(OptimizerContext.class)) {
                staticOptimizer.when(() -> OptimizerContext.getContext(anyString()))
                    .thenReturn(mock(OptimizerContext.class));
                try (MockedStatic<ExecutorContext> staticExecution = mockStatic(ExecutorContext.class)) {
                    staticExecution.when(() -> ExecutorContext.getContext(anyString()))
                        .thenReturn(mock(ExecutorContext.class));
                    when(executionContext.getSchemaName()).thenReturn("test_db");
                    ParamManager paramManager = mock(ParamManager.class);
                    when(paramManager.getBoolean(ConnectionParams.ENABLE_LOGICAL_TABLE_META)).thenReturn(true);
                    when(executionContext.getParamManager()).thenReturn(paramManager);

                    LogicalInfoSchemaContext infoSchemaContext = mock(LogicalInfoSchemaContext.class);
                    doNothing().when(infoSchemaContext).prepareContextAndRepository(any());

                    //3. handle() utils
                    SqlShowTableStatus mockNativeSqlNode = mock(SqlShowTableStatus.class);
                    when(logicalShow.getNativeSqlNode()).thenReturn(mockNativeSqlNode);

                    //4. verify
                    Cursor result = logicalShowTableStatusHandler.handle(logicalShow, executionContext);
                    Assert.assertTrue(
                        result instanceof ArrayResultCursor && ((ArrayResultCursor) result).getRows().size() == 2);
                }
            }
        }
    }

    @Test
    public void testShowSingleTableStatusWithColumnarMode()
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        LogicalShowTableStatusHandler logicalShowTableStatusHandler =
            spy(new LogicalShowTableStatusHandler(mock(IRepository.class)));
        //input parameter
        String schemaName = "test";

        //showSingleTableStatusWithColumnMode() utils
        //mock DataSource
        DataSource mockDataSource = mock(DataSource.class);
        MetaDbDataSource metaDbDataSource = mock(MetaDbDataSource.class);
        try (MockedStatic<MetaDbDataSource> mockedMetaDBDataSource = mockStatic(MetaDbDataSource.class)) {
            mockedMetaDBDataSource.when(() -> MetaDbDataSource.getInstance()).thenReturn(metaDbDataSource);
            when(metaDbDataSource.getDataSource()).thenReturn(mockDataSource);
            //mock Table MetaDB Utils
            try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = mockStatic(MetaDbUtil.class)) {
                List<ShowTablesSchemaRecord> tables = new ArrayList<>();
                tables.add(mock(ShowTablesSchemaRecord.class));
                tables.add(mock(ShowTablesSchemaRecord.class));
                mockedMetaDbUtil.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                    .thenReturn(tables);
                //mock View MetaDB Utils
                List<ViewsInfoRecord> views = new ArrayList<>();
                views.add(mock(ViewsInfoRecord.class));
                views.add(mock(ViewsInfoRecord.class));
                mockedMetaDbUtil.when(() -> MetaDbUtil.query(anyString(), any(), any())).thenReturn(views);
                //verify
                Method method = logicalShowTableStatusHandler.getClass()
                    .getDeclaredMethod("showSingleTableStatusWithColumnarMode", String.class);
                method.setAccessible(true);
                List<Object[]> result = (List<Object[]>) method.invoke(logicalShowTableStatusHandler, schemaName);
                Assert.assertTrue(result.size() == 4);
            }
        }
    }
}
