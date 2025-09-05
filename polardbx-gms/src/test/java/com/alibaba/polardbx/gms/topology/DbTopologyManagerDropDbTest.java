package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chenghui.lch
 */
public class DbTopologyManagerDropDbTest {
    private static final Logger logger = LoggerFactory.getLogger(DbTopologyManagerDropDbTest.class);

    @Test
    public void testDropDb() {

        try {

            final CreateDbInfo mockCreateDbInfo = new CreateDbInfo();
            mockCreateDbInfo.setDbName("mydb");

            final DbInfoRecord mockDbInfoRec = new DbInfoRecord();
            mockDbInfoRec.dbName = "mydb";
            mockDbInfoRec.dbStatus = DbInfoRecord.DB_STATUS_DROPPING;

            final DbInfoRecord mockDbInfoRec2 = new DbInfoRecord();
            mockDbInfoRec2.dbName = "mydb";
            mockDbInfoRec2.dbStatus = mockDbInfoRec2.DB_STATUS_RUNNING;

            List<DbInfoRecord> mockDbInfoRecList = new ArrayList<>();
            mockDbInfoRecList.add(mockDbInfoRec);

            List<DbInfoRecord> mockDbInfoRecList2 = new ArrayList<>();
            mockDbInfoRecList2.add(mockDbInfoRec2);

            final MetaDbDataSource metaDbDs = mock(MetaDbDataSource.class);
            final Connection mockMetaDbConn = mock(Connection.class);
            when(metaDbDs.getConnection()).thenReturn(mockMetaDbConn);
            try (MockedStatic<MetaDbDataSource> mockedMetaDbClass = Mockito.mockStatic(MetaDbDataSource.class)) {
                try (MockedStatic<LockUtil> mockLockUtilClass = Mockito.mockStatic(LockUtil.class)) {
                    try (MockedStatic<MetaDbUtil> mockMetaDbUtilClass = Mockito.mockStatic(MetaDbUtil.class)) {
                        mockedMetaDbClass.when(() -> MetaDbDataSource.getInstance()).thenReturn(metaDbDs);
                        mockLockUtilClass.when(() -> LockUtil.waitToAcquireMetaDbLock(any(), any()))
                            .thenAnswer(invocation -> null);
                        mockMetaDbUtilClass.when(() -> MetaDbUtil.setParameter(anyInt(), any(), any(), any()))
                            .thenAnswer(invocation -> null);
                        try {
                            mockMetaDbUtilClass.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                                .thenReturn(mockDbInfoRecList);
                            DbTopologyManager.createLogicalDb(mockCreateDbInfo);
                            Assert.fail("should throw error");
                        } catch (Throwable ex) {
                            Assert.assertTrue(ex.getMessage().contains("retry to drop it by using"));
                        }

                        try {
                            mockMetaDbUtilClass.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                                .thenReturn(mockDbInfoRecList2);
                            DbTopologyManager.createLogicalDb(mockCreateDbInfo);
                            Assert.fail("should throw error");
                        } catch (Throwable ex) {
                            Assert.assertTrue(ex.getMessage().contains("has already exist"));
                        }
                    }
                }
            }
        } catch (Throwable e) {
            logger.error(e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDropLogicalDb() {
        // Setup
        DropDbInfo dropDbInfo = new DropDbInfo();
        dropDbInfo.setDbName("testDb");
        dropDbInfo.setDropIfExists(true);
        dropDbInfo.setSocketTimeout(30000L);
        dropDbInfo.setVersionId(1L);
        dropDbInfo.setTraceId("abcdef");
        dropDbInfo.setConnId(10003L);

        final MetaDbDataSource metaDbDs = mock(MetaDbDataSource.class);
        final Connection mockMetaDbConn = mock(Connection.class);
        when(metaDbDs.getConnection()).thenReturn(mockMetaDbConn);
        try (MockedStatic<MetaDbDataSource> mockedMetaDbClass = Mockito.mockStatic(MetaDbDataSource.class)) {
            try (MockedStatic<LockUtil> mockLockUtilClass = Mockito.mockStatic(LockUtil.class)) {
                try (MockedStatic<MetaDbUtil> mockMetaDbUtilClass = Mockito.mockStatic(MetaDbUtil.class)) {
                    mockedMetaDbClass.when(() -> MetaDbDataSource.getInstance()).thenReturn(metaDbDs);
                    mockLockUtilClass.when(() -> LockUtil.waitToAcquireMetaDbLock(any(), any()))
                        .thenAnswer(invocation -> null);
                    mockMetaDbUtilClass.when(() -> MetaDbUtil.setParameter(anyInt(), any(), any(), any()))
                        .thenAnswer(invocation -> null);

                    // Execute
                    DbTopologyManager.dropLogicalDb(dropDbInfo);
                }
            }
        }
    }
}
