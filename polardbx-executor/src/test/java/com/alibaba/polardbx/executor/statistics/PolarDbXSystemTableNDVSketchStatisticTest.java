package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import org.apache.calcite.avatica.Meta;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PolarDbXSystemTableNDVSketchStatisticTest {
    /**
     * Mocked DataSource object to simulate database connection.
     */
    @Mock
    private IDataSource dataSource;

    /**
     * Mocked Connection object for database operations.
     */
    @Mock
    private IConnection conn;

    /**
     * Mocked PreparedStatement object for executing SQL statements in batches.
     */
    @Mock
    private PreparedStatement ps;

    private PolarDbXSystemTableNDVSketchStatistic polarDbXSystemTableNDVSketchStatistic;

    /**
     * Initializes mocked objects before each test case.
     */
    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.initMocks(this);
        when(dataSource.getConnection()).thenReturn(conn);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        polarDbXSystemTableNDVSketchStatistic = new PolarDbXSystemTableNDVSketchStatistic();

        Connection mockedConnection = mock(Connection.class);
        PreparedStatement mockedPreparedStatement = mock(PreparedStatement.class);

        when(mockedConnection.prepareStatement(anyString())).thenReturn(mockedPreparedStatement);
    }

    /**
     * Tests batchReplace method with a normal scenario where data is successfully replaced in PolarDB-X mode.
     */
    @Test
    public void testBatchReplaceNormalCase() throws SQLException {
        setUp();

        SystemTableNDVSketchStatistic.SketchRow sketchRow1 = mock(SystemTableNDVSketchStatistic.SketchRow.class);
        when(sketchRow1.getSchemaName()).thenReturn("schema1");
        // ... set up other mock methods for sketchRow1 ...

        SystemTableNDVSketchStatistic.SketchRow sketchRow2 = mock(SystemTableNDVSketchStatistic.SketchRow.class);
        // ... set up other mock methods for sketchRow2 ...
        when(sketchRow2.getSchemaName()).thenReturn("schema2");

        SystemTableNDVSketchStatistic.SketchRow[] sketchRows =
            new SystemTableNDVSketchStatistic.SketchRow[] {sketchRow1, sketchRow2};

        PolarDbXSystemTableNDVSketchStatistic statistic = new PolarDbXSystemTableNDVSketchStatistic();

        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
            MetaDbDataSource metaDbDataSource = mock(MetaDbDataSource.class);
            when(metaDbDataSource.getDataSource()).thenReturn(dataSource);
            metaDbDataSourceMockedStatic.when(() -> MetaDbDataSource.getInstance()).thenReturn(metaDbDataSource);
            statistic.batchReplace(sketchRows);

            // Verify that the correct number of calls were made to the PreparedStatement's setter methods
            // and that executeBatch was called once.
            verify(ps, times(2)).setString(eq(1), anyString());
            // Add more verification statements as needed ...
            verify(ps).executeBatch();
        }
    }

    /**
     * 测试用例1 - 正常情况下的删除操作
     * 设计思路：当系统处于master模式时，尝试删除指定schema下表的数据。
     * 输入参数：有效的schema名称、表名称和列名称。
     * 预期结果：成功执行SQL语句并关闭资源。
     */
    @Test
    public void testDeleteByColumnNormalCase() throws SQLException {
        // 准备
        String schemaName = "test_schema";
        String tableName = "test_table";
        String columns = "column1,column2";
        Connection c = mock(Connection.class);
        MetaDbDataSource metaDbDataSourceMock = mock(MetaDbDataSource.class);
        DataSource dataSourceMock = mock(DataSource.class);

        doReturn(dataSourceMock).when(metaDbDataSourceMock).getDataSource();
        doReturn(c).when(dataSourceMock).getConnection();
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        doReturn(preparedStatement).when(c).prepareStatement(anyString());

        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(metaDbDataSourceMock);
            // 执行
            polarDbXSystemTableNDVSketchStatistic.deleteByColumn(schemaName, tableName, columns);

            // 验证
            verify(metaDbDataSourceMock, times(1)).getDataSource();
            verify(c, times(1)).prepareStatement(anyString());
            verify(preparedStatement, times(1)).executeUpdate();
        }
    }

    /**
     * 测试用例2 - 系统不在master模式的情况
     * 设计思路：当系统不处于master模式时，直接返回而不执行任何操作。
     * 输入参数：任意schema名称、表名称和列名称。
     * 预期结果：没有执行任何数据库操作。
     */
    @Test
    public void testDeleteByColumnNotInMasterMode() {
        String schemaName = "any_schema";
        String tableName = "any_table";
        String columns = "any_column";

        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isMasterMode).thenReturn(false);

            boolean rs = polarDbXSystemTableNDVSketchStatistic.deleteByColumn(schemaName, tableName, columns, null);
            assert !rs;
        }
    }

    /**
     * 测试用例3 - 数据库操作失败的情况
     * 设计思路：模拟数据库操作过程中抛出异常。
     * 输入参数：有效的schema名称、表名称和列名称。
     * 预期结果：记录错误日志并释放资源。
     */
    @Test
    public void testDeleteByColumnWithException() throws SQLException {
        String schemaName = "test_schema";
        String tableName = "test_table";
        String columns = "column1,column2";
        Connection c = mock(Connection.class);
        DataSource metaDbDataSourceMock = mock(DataSource.class);

        doReturn(c).when(metaDbDataSourceMock).getConnection();
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        doReturn(preparedStatement).when(c).prepareStatement(anyString());

        doThrow(new SQLException()).when(preparedStatement).executeUpdate();
        boolean rs =
            polarDbXSystemTableNDVSketchStatistic.deleteByColumn(schemaName, tableName, columns, metaDbDataSourceMock);

        assert !rs;

    }
}
