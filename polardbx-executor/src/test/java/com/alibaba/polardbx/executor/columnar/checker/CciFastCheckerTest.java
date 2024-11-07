package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CciFastCheckerTest {
    @Test
    public void testCalculateColumnarChecksum() throws Throwable {
        String schema = "test";
        String table = "test";
        String index = "test";
        long tso = 100L;
        long tableId = 100L;
        long orcChecksum = 100L;
        long deletedChecksum = 200L;
        long csvChecksum = 300L;
        try (MockedStatic<ICciChecker> iCciCheckerMockedStatic = mockStatic(ICciChecker.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);) {
            // mock mock
            iCciCheckerMockedStatic.when(() -> ICciChecker.getTableId(schema, index)).thenReturn(tableId);
            List<FilesRecordSimplifiedWithChecksum> records = new ArrayList<>();
            // orc
            FilesRecordSimplifiedWithChecksum filesRecordSimplifiedWithChecksum =
                new FilesRecordSimplifiedWithChecksum();
            filesRecordSimplifiedWithChecksum.fileName = "test.orc";
            filesRecordSimplifiedWithChecksum.partitionName = "p0";
            filesRecordSimplifiedWithChecksum.commitTs = tso;
            filesRecordSimplifiedWithChecksum.checksum = orcChecksum;
            filesRecordSimplifiedWithChecksum.deletedChecksum = -1L;
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
            iCciCheckerMockedStatic.when(() -> ICciChecker.getFilesRecords(tso, tableId, schema)).thenReturn(records);

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));
            List<ColumnarAppendedFilesRecord> columnarAppendedFilesRecords = new ArrayList<>();
            ColumnarAppendedFilesRecord columnarAppendedFilesRecord = new ColumnarAppendedFilesRecord();
            columnarAppendedFilesRecord.fileName = "test.csv";
            columnarAppendedFilesRecord.partName = "p0";
            columnarAppendedFilesRecord.appendOffset = 0;
            columnarAppendedFilesRecord.appendLength = 100;
            columnarAppendedFilesRecords.add(columnarAppendedFilesRecord);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                .thenReturn(columnarAppendedFilesRecords);

            ServerThreadPool serverThreadPool = mock(ServerThreadPool.class);
            doAnswer(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    ((Runnable) args[0]).run();
                    return null;
                }
            ).when(serverThreadPool).execute(any());
            IInnerConnectionManager innerConnectionManager = mock(IInnerConnectionManager.class);
            IInnerConnection innerConnection = mock(IInnerConnection.class);
            when(innerConnectionManager.getConnection(schema)).thenReturn(innerConnection);
            Statement stmt = mock(Statement.class);
            when(innerConnection.createStatement()).thenReturn(stmt);
            when(stmt.executeQuery(any())).then(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    String sql = args[0].toString();
                    if (sql.contains("READ_ORC_ONLY=true ENABLE_OSS_DELETED_SCAN=true")) {
                        ArrayResultSet rs = new ArrayResultSet();
                        rs.getColumnName().add("checksum");
                        rs.getRows().add(new Object[] {deletedChecksum});
                        return rs;
                    } else if (sql.contains("READ_CSV_ONLY=true")) {
                        ArrayResultSet rs = new ArrayResultSet();
                        rs.getColumnName().add("checksum");
                        rs.getRows().add(new Object[] {csvChecksum});
                        return rs;
                    }
                    throw new RuntimeException();
                }
            );

            CciFastChecker checker = new CciFastChecker(schema, table, index);
            ExecutionContext ec = new ExecutionContext();

            doNothing().when(innerConnection).addExecutionContextInjectHook(any());

            DdlContext ddlContext = new DdlContext();
            ec.setDdlContext(ddlContext);

            checker.calculateColumnarChecksum(ec, 100L, serverThreadPool, innerConnectionManager);

            RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
            hash.add(orcChecksum).remove(0).remove(deletedChecksum).add(0).add(csvChecksum).remove(0);
            Assert.assertEquals(hash.getResult().longValue(), checker.getColumnarHashCode());
        }
    }

    @Test
    public void testCalculatePrimaryChecksum() throws Throwable {
        String schema = "test";
        String table = "test";
        String index = "test";
        long tso = 100L;

        ServerThreadPool serverThreadPool = mock(ServerThreadPool.class);
        when(serverThreadPool.submit(any(), any(), any(Callable.class))).then(
            invocation -> {
                Object[] args = invocation.getArguments();
                return Futures.immediateFuture(((Callable) args[2]).call());
            }
        );
        IInnerConnectionManager innerConnectionManager = mock(IInnerConnectionManager.class);
        IInnerConnection innerConnection = mock(IInnerConnection.class);
        when(innerConnectionManager.getConnection(schema)).thenReturn(innerConnection);
        Statement stmt = mock(Statement.class);
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
                } else if (sql.contains("select check_sum_v2(*) as checksum from")) {
                    ArrayResultSet rs = new ArrayResultSet();
                    rs.getColumnName().add("checksum");
                    rs.getRows().add(new Object[] {100});
                    return rs;
                }
                return new ArrayResultSet();
            }
        );

        ExecutionContext ec = new ExecutionContext();

        DdlContext ddlContext = new DdlContext();
        ec.setDdlContext(ddlContext);

        CciFastChecker checker = new CciFastChecker(schema, table, index);
        checker.calculatePrimaryChecksum(ec, tso, serverThreadPool, innerConnectionManager, null);
    }
}
