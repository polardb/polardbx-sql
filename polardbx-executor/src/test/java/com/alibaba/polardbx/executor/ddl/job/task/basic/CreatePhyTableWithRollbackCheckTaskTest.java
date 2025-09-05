package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPhyTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.FutureTask;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class CreatePhyTableWithRollbackCheckTaskTest {

    private PhysicalPlanData physicalPlanDataMock;
    private ExecutionContext ecMock;

    @Before
    public void setUp() {
        physicalPlanDataMock = mock(PhysicalPlanData.class);
        //Mockito.doNothing().when(consumerMock)
        //    .consume(any(), any(), any(), any(), any(PolarxPhysicalBackfill.TransferFileDataOperator.class));
        ecMock = mock(ExecutionContext.class);
    }

    @After
    public void tearDown() {
        Mockito.reset(physicalPlanDataMock, ecMock);
    }

    @Test
    public void testRollback1() throws Exception {
        Map<String, Set<String>> sourceTableTopology = new HashMap<>();
        Set<String> set = new HashSet<>();
        set.add("t1_00000");
        set.add("t1_00002");
        sourceTableTopology.put("g1", set);
        CreatePhyTableWithRollbackCheckTask task = spy(new CreatePhyTableWithRollbackCheckTask("s1", "t1",
            physicalPlanDataMock, sourceTableTopology));

        try (MockedStatic<ScaleOutUtils> mockedScaleOutUtils = Mockito.mockStatic(ScaleOutUtils.class);) {
            Mockito.when(ScaleOutUtils.checkTableExistence(anyString(), anyString(), anyString()))
                .thenReturn(true).thenReturn(false);
            task.genRollbackPhysicalPlans(ecMock);
        } catch (TddlNestableRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("this DDL operation cannot be rolled back"));
        }

    }

    @Test
    public void testRollback2() throws Exception {
        Map<String, Set<String>> sourceTableTopology = new HashMap<>();
        Set<String> set = new HashSet<>();
        set.add("t1_00000");
        set.add("t1_00002");
        sourceTableTopology.put("g1", set);
        CreatePhyTableWithRollbackCheckTask task = spy(new CreatePhyTableWithRollbackCheckTask("s1", "t1",
            physicalPlanDataMock, sourceTableTopology));
        DropPhyTableBuilder builder = mock(DropPhyTableBuilder.class);
        try (MockedStatic<ScaleOutUtils> mockedScaleOutUtils = Mockito.mockStatic(ScaleOutUtils.class);
            MockedStatic<DropPhyTableBuilder> mockedDropPhyTableBuilder = Mockito.mockStatic(
                DropPhyTableBuilder.class);) {
            Mockito.when(ScaleOutUtils.checkTableExistence(anyString(), anyString(), anyString()))
                .thenReturn(true).thenReturn(true);
            Mockito.when(DropPhyTableBuilder.createBuilder(anyString(), anyString(), anyBoolean(), any(TreeMap.class),
                    any(ExecutionContext.class)))
                .thenReturn(builder);
            Mockito.when(builder.build()).thenReturn(builder);
            task.genRollbackPhysicalPlans(ecMock);
        } catch (TddlNestableRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("this DDL operation cannot be rolled back"));
        }

    }
}
