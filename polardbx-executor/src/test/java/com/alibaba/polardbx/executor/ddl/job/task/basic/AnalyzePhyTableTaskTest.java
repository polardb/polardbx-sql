package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.spi.DatasourceMySQLImplement;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AnalyzePhyTableTaskTest {
    @Test
    public void testForeachPhysicalFile_success() {
        try (MockedStatic<OptimizerContext> mockedOptimizerContext = Mockito.mockStatic(OptimizerContext.class);
            MockedConstruction<DatasourceMySQLImplement> mockedDatasourceMySQLImplement = Mockito.mockConstruction(
                DatasourceMySQLImplement.class, (mock, context) -> {
                });) {
            OptimizerContext optimizerContext = Mockito.mock(OptimizerContext.class);
            ParamManager paramManager = Mockito.mock(ParamManager.class);
            mockedOptimizerContext.when(() -> OptimizerContext.getContext(anyString())).thenReturn(
                optimizerContext);
            Mockito.when(optimizerContext.getParamManager()).thenReturn(paramManager);
            Mockito.when(paramManager.getBoolean(ConnectionParams.ANALYZE_TABLE_AFTER_IMPORT_TABLESPACE)).thenReturn(
                true).thenReturn(false);
            AnalyzePhyTableTask analyzePhyTableTask = new AnalyzePhyTableTask("test", "test", "test");
            analyzePhyTableTask.duringTransaction(null, new ExecutionContext());
            analyzePhyTableTask.duringTransaction(null, new ExecutionContext());
        }
    }
}
