package com.alibaba.polardbx.executor.ddl.job.meta;

import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TableMetaChangerTest {
    @Test
    public void testTruncateTableWithRecycleBin() {
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        TddlRuleManager tddlRuleManager = mock(TddlRuleManager.class);
        TableRule tableRule = mock(TableRule.class);
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
            MockedStatic<SequenceMetaChanger> sequenceMetaChangerMockedStatic = mockStatic(SequenceMetaChanger.class);
            MockedStatic<MetaDbUtil> netaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
//        MockedStatic<SchemaManager> tableMetaChangerMockedStatic = mockStatic(TableMetaChanger.class);
        ) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext("schema")).thenReturn(optimizerContext);
            dbInfoManagerMockedStatic.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);

            when(optimizerContext.getRuleManager()).thenReturn(tddlRuleManager);
            when(tddlRuleManager.getTableRule(anyString())).thenReturn(tableRule);
            when(dbInfoManager.isNewPartitionDb(anyString())).thenReturn(false);

            TableMetaChanger.truncateTableWithRecycleBin("schema", "table", "binTable", "tmpTable", null, null);

            SchemaManager schemaManager = mock(SchemaManager.class);
            TableMeta tableMeta = mock(TableMeta.class);
            PartitionInfo partitionInfo = mock(PartitionInfo.class);
            when(dbInfoManager.isNewPartitionDb(anyString())).thenReturn(true);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable(anyString())).thenReturn(tableMeta);
            when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);
            when(partitionInfo.getPrefixTableName()).thenReturn("table");

            TableMetaChanger.truncateTableWithRecycleBin("schema", "table", "binTable", "tmpTable", null, null);
        }
    }
}
