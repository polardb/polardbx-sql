package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.FutureTask;

import static org.mockito.Mockito.*;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({PhysicalBackfillUtils.class})
public class PhysicalBackfillTaskTest {

    private BatchConsumer consumerMock;
    private ExecutionContext ecMock;

    @Before
    public void setUp() {
        consumerMock = mock(BatchConsumer.class);
        Mockito.doNothing().when(consumerMock)
            .consume(any(), any(), any(), any(), any(PolarxPhysicalBackfill.TransferFileDataOperator.class));
        ecMock = mock(ExecutionContext.class);
    }

    @After
    public void tearDown() {
        Mockito.reset(consumerMock, ecMock);
    }

    @Test
    public void testForeachPhysicalFile_success() throws Exception {
        prepareAndRunPhysicalBackfillTask(true, false, false);
        prepareAndRunPhysicalBackfillTask(true, true, false);
    }

    @Test
    public void testForeachPhysicalFile_failed() throws Exception {
        try {
            prepareAndRunPhysicalBackfillTask(false, false, false);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().indexOf("the status of BackfillBean is empty or init ") > 0);
        }
        try {
            prepareAndRunPhysicalBackfillTask(false, true, false);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().indexOf("the status of BackfillBean is empty or init ") > 0);
        }

        try {
            prepareAndRunPhysicalBackfillTask(true, false, true);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NullPointerException);
        }
        try {
            prepareAndRunPhysicalBackfillTask(true, true, true);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NullPointerException);
        }
    }

    /**
     * 准备并运行物理回填任务，根据参数模拟不同的初始化状态、加密情况及连接是否为空的情况
     *
     * @param inited 是否已初始化
     * @param encrypted 数据是否加密
     * @param connectIsNull 连接是否为null
     * @throws Exception 可能抛出的异常
     */
    public void prepareAndRunPhysicalBackfillTask(boolean inited, boolean encrypted, boolean connectIsNull)
        throws Exception {
        //OptimizerContext.getContext(schemaName)
        // Arrange
        OptimizerContext optimizerContext = new OptimizerContext("d1");
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedStatic<PhysicalBackfillUtils> mockedPhysicalBackfillUtils = Mockito.mockStatic(
            PhysicalBackfillUtils.class);
            MockedConstruction<PhysicalBackfillManager> mocked = Mockito.mockConstruction(PhysicalBackfillManager.class,
                (mock, context) -> {
                    Mockito.when(mock.loadBackfillMeta(anyLong(), anyString(), anyString(), anyString(), anyString()))
                        .thenReturn(mockBackfillBean(inited));
                }); MockedStatic<OptimizerContext> mockedOptimizerContext = Mockito.mockStatic(OptimizerContext.class)
            ; MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class);
            MockedConstruction<FutureTask> mockedFuture = Mockito.mockConstruction(FutureTask.class);) {
            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.getTempIbdFileInfo(any(Pair.class), any(Pair.class), any(Pair.class),
                        any(String.class), any(String.class), any(Pair.class), any(Long.class), any(Boolean.class),
                        any(List.class)))
                .thenAnswer(invocation -> {
                    List<Pair<Long, Long>> offsetAndSize = invocation.getArgument(8);
                    // 根据你的测试需要，对 offsetAndSize 进行修改
                    offsetAndSize.add(new Pair<>(123L, 456L));
                    return null;
                });
            Mockito.when(dbInfoManager.isNewPartitionDb(anyString()))
                .thenReturn(true);
            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);
            ParamManager paramManager = new ParamManager(new HashMap<String, String>());
            paramManager.getProps().put("PHYSICAL_BACKFILL_SPEED_LIMIT", "-1");
            optimizerContext.setParamManager(paramManager);
            mockedOptimizerContext.when(() -> OptimizerContext.getContext(anyString())).thenReturn(optimizerContext);

            PhysicalBackfillTask task = new PhysicalBackfillTask("schema", 123L, "logicalTable", "physicalTable",
                ImmutableList.of("partition1"), Pair.of("srcGroup", "srcDn"), Pair.of("srcGroup", "tarGroup"),
                ImmutableMap.of("srcGroup", Pair.of("username", "password"), "tarGroup",
                    Pair.of("username", "password")), 1000L, 0L, 0L, 500L, false, encrypted);

            XConnection conn = mock(XConnection.class);
            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.getXConnectionForStorage(anyString(), anyString(), anyInt(), anyString(),
                        anyString(), anyInt()))
                .thenReturn(connectIsNull ? null : conn);

            PolarxPhysicalBackfill.TransferFileDataOperator transferFileDataOperator =
                mock(PolarxPhysicalBackfill.TransferFileDataOperator.class);
            Mockito.when(transferFileDataOperator.getBufferLen()).thenReturn(2000l).thenReturn(1l);
            Mockito.when(conn.execReadBufferFromFile(any())).thenReturn(transferFileDataOperator);

            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.convertToCfgFileName(any(String.class), any(String.class)))
                .thenCallRealMethod();

            task.foreachPhysicalFile(Pair.of("srcDb", "srcGroup"), Pair.of("tarDb", "tarGroup"), "partition1",
                consumerMock,
                ecMock);
        }
    }

    private PhysicalBackfillManager.BackfillBean mockBackfillBean(boolean isInit) {
        return new PhysicalBackfillManager.BackfillBean(1709745034997297152l, "d1", "t1", "d1", "t1",
            mockBackfillObjectRecord(isInit));
    }

    private PhysicalBackfillManager.BackfillObjectBean mockBackfillObjectRecord(boolean isInit) {
        PhysicalBackfillDetailInfoFieldJSON detailInfo = PhysicalBackfillDetailInfoFieldJSON.fromJson(
            "{\"msg\":\"\",\"sourceHostAndPort\":{\"key\":\"26.12.155.86\",\"value\":31159},\"targetHostAndPorts\":[{\"key\":\"11.112.141.109\",\"value\":31175},{\"key\":\"26.12.156.152\",\"value\":31096}]}");
        PhysicalBackfillManager.BackfillObjectBean record =
            new PhysicalBackfillManager.BackfillObjectBean(1, 1709745034997297152l, "d1", "t1", "d1", "t1",
                "d1_p00000", "t1_za6v_00000", "", "d1_p00000_group", "d1_p00001_group", "d1_p00000/t1_za6v_00000",
                "./d1_p00000/t1_za6v_00000.ibd.TEMPFILE", "d1_p00001/t1_za6v_00000",
                "./d1_p00000/t1_za6v_00001.ibd",
                isInit ? PhysicalBackfillManager.BackfillStatus.RUNNING : PhysicalBackfillManager.BackfillStatus.INIT,
                detailInfo, 2, 65536,
                0, 2, "2024-03-27 17:10:43", "2024-03-27 17:10:43", null, 0, 0);
        return record;
    }
    // 更多测试用例，例如处理异常、不同的状态等
}
