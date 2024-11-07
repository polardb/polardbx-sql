package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.sync.ColumnarSnapshotUpdateSyncAction;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.anyLong;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarManagerTest {
    private static final List<MockFileStatus> MOCK_FILE_STATUSES = new ArrayList<>(Arrays.asList(
        new MockFileStatus("1.csv", "p1", 1, 2, 1),
        new MockFileStatus("1.orc", "p1", 1, 2, 1),
        new MockFileStatus("1.del", "p1", 1, 2, 1),
        new MockFileStatus("2.csv", "p1", 2, 4, 1),
        new MockFileStatus("2.orc", "p1", 2, 4, 1),
        new MockFileStatus("2.del", "p1", 2, 4, 1),
        new MockFileStatus("3.csv", "p1", 3, 5, 1),
        new MockFileStatus("3.orc", "p1", 3, 5, 1),
        new MockFileStatus("3.del", "p1", 3, 5, 1),
        new MockFileStatus("4.csv", "p2", 1, 5, 1),
        new MockFileStatus("4.orc", "p2", 1, 5, 1),
        new MockFileStatus("4.del", "p2", 1, 5, 1)
    ));

    private static final List<MockFileStatus> MOCK_COMPACTION_STATUSES = Arrays.asList(
        new MockFileStatus("5.csv", "p2", 2, 5, 1),
        new MockFileStatus("5.orc", "p2", 2, 5, 1),
        new MockFileStatus("5.del", "p2", 2, 5, 1)
    );

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedStatic<InstConfUtil> mockInstConfUtil;
    private MockedConstruction<FilesAccessor> mockFilesAccessorCtor;

    final Answer<List<FilesRecordSimplified>> mockDeltaFilesQuery = invocationOnMock -> {
        Object[] args = invocationOnMock.getArguments();
        long tso = (long) args[0];
        long lastTso = (long) args[1];

        // Simulate loading action from gms
        List<FilesRecordSimplified> result = new ArrayList<>();
        for (MockFileStatus fileStatus : MOCK_FILE_STATUSES) {
            if ((fileStatus.commitTso > lastTso && fileStatus.commitTso <= tso) ||
                (fileStatus.removeTso > lastTso && fileStatus.removeTso <= tso)) {

                FilesRecordSimplified filesRecordSimplified = new FilesRecordSimplified();

                filesRecordSimplified.commitTs = fileStatus.commitTso;
                if (fileStatus.removeTso <= tso) {
                    filesRecordSimplified.removeTs = fileStatus.removeTso;
                }
                filesRecordSimplified.fileName = fileStatus.fileName;
                filesRecordSimplified.partitionName = fileStatus.partitionName;
                filesRecordSimplified.schemaTs = fileStatus.schemaTso;

                result.add(filesRecordSimplified);
            }
        }
        return result;
    };

    @Before
    public void prepareFileVersionStorage() {
        try {
            mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(null);

            mockInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
            mockInstConfUtil.when(() -> InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY)).thenReturn(0);

            mockFilesAccessorCtor = Mockito.mockConstruction(FilesAccessor.class, (mock, context) -> {
                Mockito.when(mock.queryColumnarDeltaFilesByTsoAndTableId(anyLong(), anyLong(),
                        Mockito.anyString(), Mockito.anyString()))
                    .thenAnswer(mockDeltaFilesQuery);
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void reloadColumnarManager() {
        ColumnarManager.getInstance().reload();
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
        if (mockFilesAccessorCtor != null) {
            mockFilesAccessorCtor.close();
        }
        if (mockInstConfUtil != null) {
            mockInstConfUtil.close();
        }
    }

    @Test
    public void testTsoUpdate() {
        long initTso = 1L;
        long orphanTso = 2L;
        long newTso = 3L;

        // Prepare mock data
        try (MockedStatic<ColumnarTransactionUtils> columnarTransactionUtilsMockedStatic = Mockito.mockStatic(
            ColumnarTransactionUtils.class)) {
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms)
                .thenReturn(initTso);

            // Initialize columnar manager and fetch latest tso from mock gms
            ColumnarManager cm = ColumnarManager.getInstance();
            Assert.assertEquals(initTso, cm.latestTso());

            // Update latest tso by sync action
            ColumnarSnapshotUpdateSyncAction action = new ColumnarSnapshotUpdateSyncAction(newTso);
            action.sync();
            Assert.assertEquals(newTso, cm.latestTso());

            // Sync an older tso, which will be ignored
            action.setLatestTso(orphanTso);
            action.sync();
            Assert.assertEquals(newTso, cm.latestTso());
        }
    }

    @Test
    public void testReload() {
        long initTso = 1L;
        long newTso = 3L;

        // Prepare mock data
        try (MockedStatic<ColumnarTransactionUtils> columnarTransactionUtilsMockedStatic = Mockito.mockStatic(
            ColumnarTransactionUtils.class)) {
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(newTso);

            // Initialize columnar manager set mock latest tso
            ColumnarManager cm = ColumnarManager.getInstance();
            cm.setLatestTso(initTso);
            Assert.assertEquals(initTso, cm.latestTso());

            // Test RELOAD COLUMNARMANAGER
            cm.reload();
            Assert.assertEquals(newTso, cm.latestTso());
        }
    }

    @Test
    public void testPurge() {
        long newTso = 3L;
        long purgeTso = 2L;
        long minTso = 1L;

        // Prepare mock data
        try (MockedStatic<ColumnarTransactionUtils> columnarTransactionUtilsMockedStatic = Mockito.mockStatic(
            ColumnarTransactionUtils.class)) {
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(newTso);
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getMinColumnarSnapshotTime)
                .thenReturn(minTso);

            // Initialize columnar manager and set min/latest tso
            DynamicColumnarManager cm = (DynamicColumnarManager) ColumnarManager.getInstance();
            Assert.assertEquals(minTso, cm.getMinTso().longValue());
            Assert.assertEquals(newTso, cm.latestTso());

            // Test purge
            cm.purge(purgeTso);
            Assert.assertEquals(purgeTso, cm.getMinTso().longValue());
            // This purge should be ignored
            cm.purge(minTso);
            Assert.assertEquals(purgeTso, cm.getMinTso().longValue());
        }
    }

    @Test
    public void testPurgeWithMultiVersionSchema() {
        long queryTso2 = 6L;
        long newPurgeTso = 5L;
        long queryTso1 = 4L;
        long initTso = 3L;
        long purgeTso = 2L;
        long minTso = 1L;

        // Prepare mock data
        try (MockedStatic<ColumnarTransactionUtils> columnarTransactionUtilsMockedStatic = Mockito.mockStatic(
            ColumnarTransactionUtils.class)) {
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms)
                .thenReturn(initTso);
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getMinColumnarSnapshotTime)
                .thenReturn(minTso);

            FileVersionStorage mockFileVersionStorage = Mockito.mock(FileVersionStorage.class);
            MultiVersionColumnarSchema mockMultiVersionColumnarSchema = Mockito.mock(MultiVersionColumnarSchema.class);

            // Initialize columnar manager and set min/latest tso
            DynamicColumnarManager cm = (DynamicColumnarManager) ColumnarManager.getInstance();

            cm.injectForTest(mockFileVersionStorage, mockMultiVersionColumnarSchema, new AtomicLong(0L), null);
            Assert.assertEquals(minTso, cm.getMinTso().longValue());
            Assert.assertEquals(initTso, cm.latestTso());

            // Load and Query multi version schema
            Pair<List<String>, List<String>> orcAndCsvFile = cm.findFileNames(initTso, "db1", "t1", "p1");
            Assert.assertEquals(2, orcAndCsvFile.getKey().size());
            Assert.assertEquals(2, orcAndCsvFile.getValue().size());

            // Test purge
            cm.purge(purgeTso);
            Assert.assertEquals(purgeTso, cm.getMinTso().longValue());
            // This purge should be ignored
            cm.purge(minTso);
            Assert.assertEquals(purgeTso, cm.getMinTso().longValue());

            // Load and Query multi version schema for new tso
            cm.setLatestTso(queryTso1);
            Assert.assertEquals(queryTso1, cm.latestTso());

            orcAndCsvFile = cm.findFileNames(queryTso1, "db1", "t1", "p1");
            Assert.assertEquals(1, orcAndCsvFile.getKey().size());
            Assert.assertEquals(1, orcAndCsvFile.getValue().size());

            // Test for query already purged files
            cm.purge(newPurgeTso);
            Assert.assertEquals(newPurgeTso, cm.getMinTso().longValue());

            cm.setLatestTso(queryTso2);
            Assert.assertEquals(queryTso2, cm.latestTso());

            orcAndCsvFile = cm.findFileNames(queryTso2, "db1", "t1", "p1");
            Assert.assertEquals(0, orcAndCsvFile.getKey().size());
            Assert.assertEquals(0, orcAndCsvFile.getValue().size());

            // Test access snapshot which is already purged
            boolean snapshotFailed = false;
            try {
                cm.findFileNames(minTso, "db1", "t1", "p1");
            } catch (TddlRuntimeException e) {
                Assert.assertEquals(
                    "ERR-CODE: [TDDL-12005][ERR_COLUMNAR_SNAPSHOT] Failed to generate columnar snapshot of tso: 1 ",
                    e.getMessage()
                );
                snapshotFailed = true;
            }
            Assert.assertTrue(snapshotFailed);
        }
    }

    @Test
    public void testTsoBackward() {
        long minTso = 1L;
        long initTso = 3L;
        long queryTso1 = 4L;
        // Prepare mock data
        try (MockedStatic<ColumnarTransactionUtils> columnarTransactionUtilsMockedStatic = Mockito.mockStatic(
            ColumnarTransactionUtils.class);
            MockedConstruction<ColumnarCheckpointsAccessor> checkpointsAccessorCtor = Mockito.mockConstruction(
                ColumnarCheckpointsAccessor.class, (mock, context) -> {
                    Mockito.when(mock.queryValidCheckpointByTso(anyLong()))
                        .thenAnswer(invocation -> {
                            Long tso = invocation.getArgument(0);
                            ColumnarCheckpointsRecord record = new ColumnarCheckpointsRecord();
                            if (3L == tso) {
                                record.minCompactionTso = 2;
                            } else {
                                record.minCompactionTso = 0;
                            }
                            return Collections.singletonList(record);
                        });
                }
            )) {
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms)
                .thenReturn(initTso);
            columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getMinColumnarSnapshotTime)
                .thenReturn(minTso);

            FileVersionStorage mockFileVersionStorage = Mockito.mock(FileVersionStorage.class);
            MultiVersionColumnarSchema mockMultiVersionColumnarSchema = Mockito.mock(MultiVersionColumnarSchema.class);

            // Initialize columnar manager and set min/latest tso
            DynamicColumnarManager cm = (DynamicColumnarManager) ColumnarManager.getInstance();

            cm.injectForTest(mockFileVersionStorage, mockMultiVersionColumnarSchema, new AtomicLong(0), null);
            Assert.assertEquals(minTso, cm.getMinTso().longValue());
            Assert.assertEquals(initTso, cm.latestTso());

            // Load and Query multi version schema
            Pair<List<String>, List<String>> orcAndCsvFile = cm.findFileNames(initTso, "db1", "t1", "p1");
            Assert.assertEquals(2, orcAndCsvFile.getKey().size());
            Assert.assertEquals(2, orcAndCsvFile.getValue().size());

            orcAndCsvFile = cm.findFileNames(initTso, "db1", "t1", "p2");
            Assert.assertEquals(1, orcAndCsvFile.getKey().size());
            Assert.assertEquals(1, orcAndCsvFile.getValue().size());

            // Simulate TSO backward: 3 -> 2
            MOCK_FILE_STATUSES.addAll(MOCK_COMPACTION_STATUSES);

            cm.setLatestTso(queryTso1);
            Assert.assertEquals(queryTso1, cm.latestTso());

            orcAndCsvFile = cm.findFileNames(initTso, "db1", "t1", "p2");
            Assert.assertEquals(1, orcAndCsvFile.getKey().size());
            Assert.assertEquals(1, orcAndCsvFile.getValue().size());

            orcAndCsvFile = cm.findFileNames(queryTso1, "db1", "t1", "p2");
            Assert.assertEquals(2, orcAndCsvFile.getKey().size());
            Assert.assertEquals(2, orcAndCsvFile.getValue().size());

            orcAndCsvFile = cm.findFileNames(queryTso1, "db1", "t1", "p1");
            Assert.assertEquals(1, orcAndCsvFile.getKey().size());
            Assert.assertEquals(1, orcAndCsvFile.getValue().size());
        }
    }

    @Test
    public void testGetTsoWithDelayMinValue() throws SQLException {
        mockInstConfUtil.when(() -> InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY)).thenReturn(1000);
        Connection mockConnection = Mockito.mock(Connection.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(mockPreparedStatement);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        Assert.assertEquals(Long.MIN_VALUE, ColumnarManager.getInstance().latestTso());

        ColumnarManager.getInstance().reload();

        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getLong("checkpoint_tso")).thenReturn(1L);
        Assert.assertEquals(1L, ColumnarManager.getInstance().latestTso());
    }

    static class MockFileStatus {
        String fileName;
        String partitionName;
        long commitTso;
        long removeTso;
        long schemaTso;

        MockFileStatus(String fileName, String partitionName, long commitTso, long removeTso, long schemaTso) {
            this.fileName = fileName;
            this.partitionName = partitionName;
            this.commitTso = commitTso;
            this.removeTso = removeTso;
            this.schemaTso = schemaTso;
        }
    }

}
