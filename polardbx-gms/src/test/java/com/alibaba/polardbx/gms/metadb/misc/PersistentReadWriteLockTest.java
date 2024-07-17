package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PersistentReadWriteLockTest {

    @Test
    public void testTryReadWriteLockBatch() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);
            mockedMetaDbUtil.when(() -> MetaDbUtil.insert(anyString(), anyList(), any(Connection.class)))
                .thenReturn(new int[] {1});

            mockedMetaDbUtil.when(() -> MetaDbUtil.tryGetLock(any(Connection.class), any(String.class), anyLong()))
                .thenCallRealMethod();
            mockedMetaDbUtil.when(() -> MetaDbUtil.releaseLock(any(Connection.class), any(String.class)))
                .thenCallRealMethod();

            ResultSet lockRs = mock(ResultSet.class);
            when(lockRs.next()).thenReturn(true);
            when(lockRs.getInt(1)).thenReturn(0).thenReturn(1);
            when(statement.executeQuery(any())).thenReturn(lockRs);

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner", new HashSet<>(), new HashSet<>());
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner", ImmutableSet.of("d1"),
                new HashSet<>());

            ResultSet emptyLockRs = mock(ResultSet.class);
            when(emptyLockRs.next()).thenReturn(true).thenReturn(false);
            when(emptyLockRs.getInt(1)).thenThrow(new SQLException());
            when(statement.executeQuery(any())).thenReturn(emptyLockRs);
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner", ImmutableSet.of("d1"),
                new HashSet<>());
        }
    }
}
