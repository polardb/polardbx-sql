package com.alibaba.polardbx.executor.ddl.newengine.backfill;

import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.handler.GsiBackfillHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BackfillHandlerTest {

    @Test
    public void testGetPrimaryTableName() {
        ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = Mockito.mock(GsiMetaManager.GsiTableMetaBean.class);
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = Mockito.mock(GsiMetaManager.GsiIndexMetaBean.class);

        Mockito.when(executionContext.getSchemaManager(Mockito.anyString())).thenReturn(schemaManager);
        Mockito.when(schemaManager.getTable(Mockito.anyString())).thenReturn(tableMeta);
        Mockito.when(tableMeta.isGsi()).thenReturn(true);
        Mockito.when(tableMeta.getGsiTableMetaBean()).thenReturn(gsiTableMetaBean);
        gsiTableMetaBean.gsiMetaBean = gsiIndexMetaBean;

        String tableName = GsiBackfillHandler.getPrimaryTableName("wumu", "gsi", true, executionContext);
        Assert.assertNull(tableName);
    }
}
