package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.client.LocalStatementClient;
import com.alibaba.polardbx.executor.mpp.client.MppResultCursor;
import com.alibaba.polardbx.executor.mpp.client.MppRunner;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.MPPQueryMonitor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MPPQueryMonitorTest {
    @Test
    public void test() {

        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<GmsNodeManager> mockedGmsNodeManager = Mockito.mockStatic(GmsNodeManager.class);
            MockedStatic<MppRunner> mockedMppRunner = Mockito.mockStatic(MppRunner.class)
        ) {

            mockedConfigDataMode.when(() -> ConfigDataMode.isMasterMode()).thenReturn(true);
            GmsNodeManager gmsNodeManager = Mockito.mock(GmsNodeManager.class);
            mockedGmsNodeManager.when(() -> GmsNodeManager.getInstance()).thenReturn(gmsNodeManager);

            // three nodes in master cluster.
            List<GmsNodeManager.GmsNode> nodes = new ArrayList<>();
            nodes.add(Mockito.mock(GmsNodeManager.GmsNode.class));
            nodes.add(Mockito.mock(GmsNodeManager.GmsNode.class));
            nodes.add(Mockito.mock(GmsNodeManager.GmsNode.class));
            Mockito.when(gmsNodeManager.getNodesBySyncScope(Mockito.any(SyncScope.class))).thenReturn(nodes);

            MPPQueryMonitor mppQueryMonitor = MPPQueryMonitor.getInstance();

            Map extraCmd = new HashMap();
            extraCmd.put(ConnectionParams.COLUMNAR_CLUSTER_MAXIMUM_QPS.getName(), 1000);
            extraCmd.put(ConnectionParams.COLUMNAR_CLUSTER_MAXIMUM_CONCURRENCY.getName(), 8);
            extraCmd.put(ConnectionParams.COLUMNAR_QPS_WINDOW_PERIOD.getName(), 1000);
            ParamManager paramManager = new ParamManager(extraCmd);
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.setParamManager(paramManager);
            executionContext.setMemoryPool(Mockito.mock(MemoryPool.class));
            executionContext.setQuerySpillSpaceMonitor(Mockito.mock(QuerySpillSpaceMonitor.class));

            RelNode plan = Mockito.mock(RelNode.class);
            MppRunner mppRunner = Mockito.mock(MppRunner.class);
            mockedMppRunner.when(() -> MppRunner.create(Mockito.any(), Mockito.any())).thenReturn(mppRunner);

            MppResultCursor mppResultCursor =
                new MppResultCursor(Mockito.mock(LocalStatementClient.class), executionContext, Mockito.mock(
                    CursorMeta.class));

            Mockito.when(mppRunner.execute()).thenReturn(mppResultCursor);
            Cursor cursor = ExecutorHelper.executeCluster(plan, executionContext);
            System.out.println(mppQueryMonitor.getQueryConcurrency());
            cursor.close(null);

            System.out.println(mppQueryMonitor.calculateQPS(
                executionContext.getParamManager().getLong(ConnectionParams.COLUMNAR_QPS_WINDOW_PERIOD)));
            System.out.println(mppQueryMonitor.getQueryConcurrency());
        }

    }

}