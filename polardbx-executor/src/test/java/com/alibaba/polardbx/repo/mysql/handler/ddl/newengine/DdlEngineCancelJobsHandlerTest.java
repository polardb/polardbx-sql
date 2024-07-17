package com.alibaba.polardbx.repo.mysql.handler.ddl.newengine;

import com.alibaba.polardbx.common.ddl.newengine.DdlPlanState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.partitionmanagement.rebalance.RebalanceDdlPlanManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DdlEngineCancelJobsHandlerTest {

    @Test
    public void cancelTest()
        throws Exception {
        DdlEngineRecord ddlEngineRecord = new DdlEngineRecord();
        ddlEngineRecord.state = "ROLLBACK_RUNNING";
        ddlEngineRecord.ddlType = "REBALANCE";
        ddlEngineRecord.ddlStmt = "REBALANCE DATABASE drain_node='dn1'";
        MyRepository repository = mock(MyRepository.class);
        ExecutionContext ecMock = mock(ExecutionContext.class);
        //OptimizerContext.getContext(schemaName)
        // Arrange
        OptimizerContext optimizerContext = new OptimizerContext("d1");
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedConstruction<DdlEngineSchedulerManager> mocked = Mockito.mockConstruction(
            DdlEngineSchedulerManager.class,
            (mock, context) -> {
                Mockito.when(mock.fetchRecordByJobId(anyLong()))
                    .thenReturn(ddlEngineRecord);
            }); MockedConstruction<RebalanceDdlPlanManager> mockedRebalanceDdlPlanManager = Mockito.mockConstruction(
            RebalanceDdlPlanManager.class, (mock, context) -> {
                Mockito.doNothing().when(mock)
                    .updateRebalanceScheduleState(anyLong(), any(DdlPlanState.class), anyString());
            });) {
            DdlEngineCancelJobsHandler ddlEngineCancelJobsHandler = new DdlEngineCancelJobsHandler(repository);
            ParamManager paramManager = new ParamManager(new HashMap());
            optimizerContext.setParamManager(paramManager);

            Mockito.when(ecMock.getParamManager()).thenReturn(paramManager);
            paramManager.getProps().put("CANCEL_REBALANCE_JOB_DUE_MAINTENANCE", "false");
            try {
                ddlEngineCancelJobsHandler.doCancel(123l, ecMock);
            } catch (TddlRuntimeException ex) {
                Assert.assertTrue(ex.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
            ddlEngineRecord.ddlStmt = "REBALANCE DATABASE";
            try {
                ddlEngineCancelJobsHandler.doCancel(123l, ecMock);
            } catch (TddlRuntimeException ex) {
                Assert.assertTrue(ex.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
            paramManager.getProps().put("CANCEL_REBALANCE_JOB_DUE_MAINTENANCE", "true");
            try {
                ddlEngineCancelJobsHandler.doCancel(123l, ecMock);
            } catch (TddlRuntimeException ex) {
                Assert.assertTrue(ex.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
        }
    }
}
