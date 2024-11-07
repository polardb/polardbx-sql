package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CciIncrementalCheckerTest {
    @Test
    public void test() throws Throwable {
        String schema = "test";
        String table = "test";
        String index = "test";
        long tsoV0 = 100L;
        long tsoV1 = 200L;

        // Mock executor.
        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(schema, executorContext);
        IInnerConnectionManager innerConnectionManager = mock(IInnerConnectionManager.class);
        executorContext.setInnerConnectionManager(innerConnectionManager);
        ITopologyExecutor topologyExecutor = mock(ITopologyExecutor.class);
        executorContext.setTopologyExecutor(topologyExecutor);
        ServerThreadPool serverThreadPool = mock(ServerThreadPool.class);
        when(topologyExecutor.getExecutorService()).thenReturn(serverThreadPool);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        when(serverThreadPool.submit(any(), any(), any(Callable.class))).then(
            invocation -> {
                Object[] args = invocation.getArguments();
                return threadPool.submit((Callable) args[2]);
            }
        );
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CCI_INCREMENTAL_CHECK_PARALLELISM, "1");

        // Mock result set.
        Statement stmt = mock(Statement.class);
        IInnerConnection innerConnection = mock(IInnerConnection.class);
        when(innerConnectionManager.getConnection(schema)).thenReturn(innerConnection);
        when(innerConnection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).then(
            invocation -> {
                Object[] args = invocation.getArguments();
                String sql = args[0].toString();
                if (sql.contains("show topology from")) {
                    ArrayResultSet rs = new ArrayResultSet();
                    rs.getColumnName().add("GROUP_NAME");
                    rs.getColumnName().add("TABLE_NAME");
                    rs.getRows().add(new Object[] {"test_group", "test_table"});
                    return rs;
                } else if (sql.contains("select * from " + table)) {
                    MockResultSet rs = new MockResultSet();
                    rs.getColumnName().add("id");
                    rs.getColumnName().add("a");
                    rs.getRows().add(new Object[] {100, 100});
                    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
                    rs.setMetaData(metaData);
                    when(metaData.getColumnCount()).thenReturn(2);
                    when(metaData.getColumnName(1)).thenReturn("a");
                    when(metaData.getColumnName(2)).thenReturn("id");
                    return rs;
                } else if (sql.contains("select count(0) from test as of tso 200 where (a,id) in")) {
                    ArrayResultSet rs = new ArrayResultSet();
                    rs.getColumnName().add("count");
                    rs.getRows().add(new Object[] {1});
                    return rs;
                }
                return new ArrayResultSet();
            }
        );

        // Mock v0 files.
        List<FilesRecordSimplifiedWithChecksum> v0Records = mockV0Files();
        List<FilesRecordSimplifiedWithChecksum> v1Records = mockV1Files();
        List<ColumnarAppendedFilesRecord> v1AppendedFilesRecords = mockV1AppendedFilesRecords();

        ExecutionContext ec = new ExecutionContext();
        DdlContext ddlContext = new DdlContext();
        ec.setDdlContext(ddlContext);

        try (MockedStatic<ICciChecker> iCciCheckerMockedStatic = mockStatic(ICciChecker.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);) {
            iCciCheckerMockedStatic.when(() -> ICciChecker.getTableId(schema, index)).thenReturn(100L);
            iCciCheckerMockedStatic.when(() -> ICciChecker.getFilesRecords(tsoV0, 100L, schema)).thenReturn(v0Records);
            iCciCheckerMockedStatic.when(() -> ICciChecker.getFilesRecords(tsoV1, 100L, schema)).thenReturn(v1Records);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                .thenReturn(v1AppendedFilesRecords);

            CciIncrementalChecker checker = new CciIncrementalChecker(schema, table, index);
            checker.check(ec, tsoV0, tsoV1, tsoV1);

            List<String> reports = new ArrayList<>();
            checker.getCheckReports(reports);
            System.out.println(reports);
            Assert.assertTrue(reports.size() == 1 && reports.get(0).contains(
                "Incremental check passed for schema test, table test, index test , increment insert count 1"));
        }

        threadPool.shutdown();
    }

    private static List<FilesRecordSimplifiedWithChecksum> mockV0Files() {
        List<FilesRecordSimplifiedWithChecksum> records = new ArrayList<>();
        // orc
        FilesRecordSimplifiedWithChecksum filesRecordSimplifiedWithChecksum =
            new FilesRecordSimplifiedWithChecksum();
        filesRecordSimplifiedWithChecksum.fileName = "test_v0.orc";
        filesRecordSimplifiedWithChecksum.partitionName = "p0";
        records.add(filesRecordSimplifiedWithChecksum);
        return records;
    }

    private static List<FilesRecordSimplifiedWithChecksum> mockV1Files() {
        List<FilesRecordSimplifiedWithChecksum> records = new ArrayList<>();
        // orc
        FilesRecordSimplifiedWithChecksum filesRecordSimplifiedWithChecksum =
            new FilesRecordSimplifiedWithChecksum();
        filesRecordSimplifiedWithChecksum.fileName = "test_v0.orc";
        filesRecordSimplifiedWithChecksum.partitionName = "p0";
        records.add(filesRecordSimplifiedWithChecksum);
        filesRecordSimplifiedWithChecksum =
            new FilesRecordSimplifiedWithChecksum();
        filesRecordSimplifiedWithChecksum.fileName = "test_v1.orc";
        filesRecordSimplifiedWithChecksum.partitionName = "p0";
        records.add(filesRecordSimplifiedWithChecksum);
        // csv
        filesRecordSimplifiedWithChecksum = new FilesRecordSimplifiedWithChecksum();
        filesRecordSimplifiedWithChecksum.fileName = "test.csv";
        filesRecordSimplifiedWithChecksum.partitionName = "p0";
        records.add(filesRecordSimplifiedWithChecksum);
        // del
        filesRecordSimplifiedWithChecksum = new FilesRecordSimplifiedWithChecksum();
        filesRecordSimplifiedWithChecksum.fileName = "test.del";
        filesRecordSimplifiedWithChecksum.partitionName = "p0";
        records.add(filesRecordSimplifiedWithChecksum);
        return records;
    }

    private List<ColumnarAppendedFilesRecord> mockV1AppendedFilesRecords() {
        List<ColumnarAppendedFilesRecord> records = new ArrayList<>();
        ColumnarAppendedFilesRecord record = new ColumnarAppendedFilesRecord();
        record.setFileName("test.csv");
        record.setPartName("p0");
        record.setAppendOffset(0);
        record.setAppendLength(100);
        records.add(record);

        record = new ColumnarAppendedFilesRecord();
        record.setFileName("test.del");
        record.setPartName("p0");
        record.setAppendOffset(0);
        record.setAppendLength(100);
        records.add(record);
        return records;
    }

    private static class MockResultSet extends ArrayResultSet {
        ResultSetMetaData metaData;

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            return metaData;
        }

        public void setMetaData(ResultSetMetaData metaData) {
            this.metaData = metaData;
        }
    }
}
