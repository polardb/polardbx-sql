package com.alibaba.polardbx.executor.ddl.newengine.dag;

import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DdlEngineRequesterTest {

    @Test
    public void testExecute() {
        DdlJob ddlJob = Mockito.mock(DdlJob.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        DdlContext ddlContext = Mockito.mock(DdlContext.class);

        Mockito.when(ddlContext.getSchemaName()).thenReturn("information_schema");

        DdlEngineRequester requester = new DdlEngineRequester(ddlJob, ec, ddlContext);

        try {
            requester.execute();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("The DDL job can not be executed"));
        }
    }
}
