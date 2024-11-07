package com.alibaba.polardbx.executor.scaleout;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.TddlGroupExecutor;
import com.alibaba.polardbx.executor.TopologyExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPhyTableBuilder;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ScaleOutUtilsTest {
    @Test
    public void checkTableExistenceTest() {
        try (MockedStatic<DropPhyTableBuilder> mockedDropPhyTableBuilder = Mockito.mockStatic(
            DropPhyTableBuilder.class);
            MockedStatic<DbGroupInfoManager> mockDbGroupInfoManager = Mockito.mockStatic(
                DbGroupInfoManager.class);
            MockedStatic<ExecutorContext> mockExecutorContext = Mockito.mockStatic(
                ExecutorContext.class);) {

            ExecutorContext mockEc = Mockito.mock(ExecutorContext.class);
            TopologyExecutor mockTopologyExecutor = Mockito.mock(TopologyExecutor.class);
            TddlGroupExecutor mockTddlGroupExecutor = Mockito.mock(TddlGroupExecutor.class);
            TGroupDataSource mockTddlGroupDataSource = Mockito.mock(TGroupDataSource.class);
            TGroupDirectConnection mockTddlGroupDirectConnection = Mockito.mock(TGroupDirectConnection.class);
            PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
            ResultSet mockRs = Mockito.mock(ResultSet.class);

            DbGroupInfoRecord DbGroupInfoRecord = new DbGroupInfoRecord();
            DbGroupInfoRecord.phyDbName = "ddd";
            DbGroupInfoManager mockDbGroupInfoMan = Mockito.mock(DbGroupInfoManager.class);
            mockDbGroupInfoManager.when(() -> DbGroupInfoManager.getInstance()).thenReturn(mockDbGroupInfoMan);
            mockExecutorContext.when(() -> ExecutorContext.getContext(anyString())).thenReturn(mockEc)
                .thenReturn(mockEc);

            Mockito.when(mockEc.getTopologyExecutor()).thenReturn(mockTopologyExecutor);
            Mockito.when(mockTopologyExecutor.getGroupExecutor(anyString())).thenReturn(mockTddlGroupExecutor);
            Mockito.when(mockTddlGroupExecutor.getDataSource()).thenReturn(mockTddlGroupDataSource);
            Mockito.when(mockTddlGroupDataSource.getConnection()).thenReturn(mockTddlGroupDirectConnection);
            Mockito.when(mockTddlGroupDirectConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
            Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);
            Mockito.when(mockRs.next()).thenReturn(true).thenReturn(false);
            Mockito.when(mockDbGroupInfoMan.queryGroupInfo(anyString(), anyString())).thenReturn(DbGroupInfoRecord);

            Assert.assertTrue(ScaleOutUtils.checkTableExistence("test", "test", "test"));
            Assert.assertFalse(ScaleOutUtils.checkTableExistence("test", "test", "test"));

            mockExecutorContext.when(() -> ExecutorContext.getContext(anyString())).thenReturn(null).thenReturn(mockEc);
            try {
                ScaleOutUtils.checkTableExistence("test", "test", "test");
            } catch (TddlNestableRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("existence failed"));
            }

            Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(null);
            try {
                ScaleOutUtils.checkTableExistence("test", "test", "test");
            } catch (TddlNestableRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Error checking table existence"));
            }
        } catch (TddlNestableRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("this DDL operation cannot be rolled back"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
