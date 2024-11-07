package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SequenceBean;
import org.junit.Ignore;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type.NEW;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type.TIME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class TruncateTableBuildJobTest {

    @Ignore
    public void testResetSequence4TruncateTableTask() {
        Connection metaDbConn = Mockito.mock(Connection.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);

        try (MockedStatic<SequenceUtil> mockedSequenceUtil = Mockito.mockStatic(SequenceUtil.class);
            MockedStatic<SyncManagerHelper> mockedSyncManagerHelper = Mockito.mockStatic(SyncManagerHelper.class)) {
            mockedSequenceUtil.when(
                    () -> SequenceUtil.resetSequence4TruncateTable(anyString(), anyString(), any(), any()))
                .thenAnswer(invocation -> {
                    return null;
                });
            mockedSyncManagerHelper.when(() -> SyncManagerHelper.sync(
                    any(IGmsSyncAction.class), anyString(), any(SyncScope.class)
                )
            ).thenReturn(ImmutableList.of());

            ResetSequence4TruncateTableTask task = new ResetSequence4TruncateTableTask("testSchema", "testTb");
            task.duringTransaction(metaDbConn, ec);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Ignore
    public void testResetSequence4TruncateTableTaskSyncFailed() {
        Connection metaDbConn = Mockito.mock(Connection.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);

        try (MockedStatic<SequenceUtil> mockedSequenceUtil = Mockito.mockStatic(SequenceUtil.class);
            MockedStatic<SyncManagerHelper> mockedSyncManagerHelper = Mockito.mockStatic(SyncManagerHelper.class)) {
            mockedSequenceUtil.when(
                    () -> SequenceUtil.resetSequence4TruncateTable(anyString(), anyString(), any(), any()))
                .thenAnswer(invocation -> {
                    return null;
                });
            mockedSyncManagerHelper.when(() -> SyncManagerHelper.sync(
                    any(IGmsSyncAction.class), anyString(), any(SyncScope.class)
                )
            ).thenThrow(new TddlNestableRuntimeException());

            ResetSequence4TruncateTableTask task = new ResetSequence4TruncateTableTask("testSchema", "testTb");
            task.duringTransaction(metaDbConn, ec);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Test
    public void testResetSequence4TruncateTableUtil() {
        Connection metaDbConn = Mockito.mock(Connection.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        ParamManager pm = Mockito.mock(ParamManager.class);

        SequenceManagerProxy proxy = Mockito.mock(SequenceManagerProxy.class);

        Mockito.when(ec.getParamManager()).thenReturn(pm);
        Mockito.when(pm.getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE)).thenReturn(0L);

        try (MockedStatic<SequenceManagerProxy> mockedSequenceManagerProxy = Mockito.mockStatic(
            SequenceManagerProxy.class)) {
            mockedSequenceManagerProxy.when(() -> SequenceManagerProxy.getInstance()).thenReturn(proxy);
            Mockito.when(proxy.checkIfExists(anyString(), anyString())).thenReturn(NEW);

            SequenceUtil.resetSequence4TruncateTable("testSchema", "tb", metaDbConn, ec);

        } catch (TddlRuntimeException ignore) {
        }

        ParamManager pm2 = Mockito.mock(ParamManager.class);
        Mockito.when(pm2.getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE)).thenReturn(100L);
        Mockito.when(ec.getParamManager()).thenReturn(pm2);
        try (MockedStatic<SequenceManagerProxy> mockedSequenceManagerProxy = Mockito.mockStatic(
            SequenceManagerProxy.class)) {
            mockedSequenceManagerProxy.when(() -> SequenceManagerProxy.getInstance()).thenReturn(proxy);
            Mockito.when(proxy.checkIfExists(anyString(), anyString())).thenReturn(TIME);

            SequenceUtil.resetSequence4TruncateTable("testSchema", "tb", metaDbConn, ec);

        } catch (TddlRuntimeException ignore) {
        }

    }

}
