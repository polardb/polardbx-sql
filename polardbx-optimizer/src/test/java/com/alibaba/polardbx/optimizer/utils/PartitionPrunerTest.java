package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepOp;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author chenghui.lch
 */
public class PartitionPrunerTest {
    @Test
    public void testPartPrunerPruning() {
//        try(MockedStatic<PartitionPruneStepBuilder> partitionPruneStepBuilderMockedStatic = mockStatic(PartitionPruneStepBuilder.class)) {
//
//            try (MockedStatic<PartitionPruner> partitionPrunerMockedStatic = mockStatic(PartitionPruner.class)) {
//
//                try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
//                    ExecutionContext ec = mock(ExecutionContext.class);
//                    ITransaction trx = mock(ITransaction.class);
//                    ec.setTransaction(trx);
//                    String schemaName = "test";
//                    List<String> logTblNames = Collections.singletonList("test");
//
//                    DbInfoManager dm = mock(DbInfoManager.class);
//                    when(dm.isNewPartitionDb(schemaName)).thenReturn(true);
//                    dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dm);
//
//                    SchemaManager sm = mock(SchemaManager.class);
//                    ec.setSchemaManager(schemaName, sm);
//                    TableMeta tbMeta = mock(TableMeta.class);
//                    when(sm.getTable(anyString())).thenReturn(tbMeta);
//
//                    TddlRuleManager ruleManager = mock(TddlRuleManager.class);
//                    when(sm.getTddlRuleManager()).thenReturn(ruleManager);
//
//                    PartitionInfoManager partitionInfoManager = mock(PartitionInfoManager.class);
//                    when(ruleManager.getPartitionInfoManager()).thenReturn(partitionInfoManager);
//                    when(partitionInfoManager.isPartitionedTable(any())).thenReturn(true);
//                    PartitionInfo partInfo = mock(PartitionInfo.class);
//                    when(partitionInfoManager.getPartitionInfo(any())).thenReturn(partInfo);
//                    when(tbMeta.getNewPartitionInfo()).thenReturn(partInfo);
//                    PartSpecSearcher specSearcher = mock(PartSpecSearcher.class);
//                    when(partInfo.getPartSpecSearcher()).thenReturn(specSearcher);
//                    when(specSearcher.getPartIntraGroupConnKey(any(), any())).thenReturn(PartSpecSearcher.NO_FOUND_PART_SPEC);
//                    ParamManager paramManager = mock(ParamManager.class);
//                    ec.setParamManager(paramManager);
//                    when(paramManager.getBoolean(ConnectionParams.ENABLE_LOG_GROUP_CONN_KEY)).thenReturn(false);
//                    when(ec.getParamManager()).thenReturn(paramManager);
//                    when(paramManager.getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM)).thenReturn(true);
//                    when(ec.getGroupParallelism()).thenReturn(8L);
//                    when(ec.getTransaction()).thenReturn(trx);
//                    when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO);
//                    when(ec.getSchemaManager("test")).thenReturn(sm);
//                    when(sm.getTable(any())).thenReturn(tbMeta);
//
//                    PartPrunedResult prunedResult = mock(PartPrunedResult.class);
//                    PartitionPruneStepOp stepOp = mock(PartitionPruneStepOp.class);
//                    partitionPrunerMockedStatic.when(() -> PartitionPruner.checkIfPrunedResultsBitSetTheSame(any())).thenReturn(false);
//                    partitionPrunerMockedStatic.when(() -> PartitionPruner.doPruningByStepInfo(stepOp, ec)).thenReturn(prunedResult);
////
//                    // PhyTableOperation
//                    try {
//                        LogicalView op = mock(LogicalView.class);
//                        when(op.getSchemaName()).thenReturn(schemaName);
//                        when(op.getTableNames()).thenReturn(logTblNames);
//                        when(op.useSelectPartitions()).thenReturn(false);
//                        when(op.isJoin()).thenReturn(true);
//                        RelShardInfo relShardInfo = mock(RelShardInfo.class);
//                        when(op.getRelShardInfo(0, ec)).thenReturn(relShardInfo);
//                        when(relShardInfo.getPartPruneStepInfo()).thenReturn(stepOp);
//
//                        PartitionPruner.prunePartitions(op,ec);
////                        List<PartPrunedResult> results = partitionPrunerMockedStatic.
////                        Assert.assertTrue(results.size() > 0);
//                    } catch (Throwable ex) {
//                        ex.printStackTrace();
//                        String msg = ex.getMessage();
//                        Assert.assertTrue(msg.contains("invalid group or physical table names"));
//                    }
//
//                }
//            }
//        }
    }

}
