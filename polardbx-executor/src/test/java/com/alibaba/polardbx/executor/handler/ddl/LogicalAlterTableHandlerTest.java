package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.RowType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RebuildTablePrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalAlterTableHandlerTest {

    @Test
    public void testBuildIndexDefinition4Auto() {
        // 准备参数
        try (MockedStatic<AlterRepartitionUtils> mockedAlterRepartitionUtils = Mockito.mockStatic(
            AlterRepartitionUtils.class);
            MockedStatic<SqlConverter> mockedSqlConverter = Mockito.mockStatic(SqlConverter.class);
            MockedStatic<PlannerContext> mockedPlannerContext = Mockito.mockStatic(PlannerContext.class);
            MockedStatic<TddlSqlToRelConverter> mockedTddlSqlToRelConverter = Mockito.mockStatic(
                TddlSqlToRelConverter.class)) {
            LogicalAlterTableHandler logicalAlterTableHandler =
                spy(new LogicalAlterTableHandler(mock(IRepository.class)));

            GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = mock(GsiMetaManager.GsiTableMetaBean.class);
            GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = mock(GsiMetaManager.GsiIndexMetaBean.class);
            gsiTableMetaBean.gsiMetaBean = gsiIndexMetaBean;

            AlterTablePreparedData alterTablePreparedData = mock(AlterTablePreparedData.class);
            RebuildTablePrepareData rebuildTablePrepareData = spy(new RebuildTablePrepareData());
            SqlAlterTable sqlAlterTable = mock(SqlAlterTable.class);
            AlterTableWithGsiPreparedData alterTableWithGsiPreparedData = mock(AlterTableWithGsiPreparedData.class);
            AlterTable alterTable = mock(AlterTable.class);
            SqlIndexDefinition sqlIndexDefinition = mock(SqlIndexDefinition.class);
            PartitionByDefinition partitionByDefinition = mock(PartitionByDefinition.class);
            BaseDdlOperation baseDdlOperation = mock(BaseDdlOperation.class);
            SqlConverter sqlConverter = mock(SqlConverter.class);
            SqlPartitionBy sqlPartitionBy = mock(SqlPartitionBy.class);
            TableGroupRecord tableGroupRecord = mock(TableGroupRecord.class);
            ExecutionContext executionContext = mock(ExecutionContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            TddlRuleManager tddlRuleManager = mock(TddlRuleManager.class);
            TableGroupInfoManager tableGroupInfoManager = mock(TableGroupInfoManager.class);
            TableGroupConfig tableGroupConfig = mock(TableGroupConfig.class);
            TableMeta tableMeta = mock(TableMeta.class);
            PartitionInfo partitionInfo = mock(PartitionInfo.class);
            when(executionContext.getSchemaManager(any())).thenReturn(schemaManager);
            when(executionContext.getSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable(any())).thenReturn(tableMeta);
            when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);
            when(tableMeta.getGsiTableMetaBean()).thenReturn(gsiTableMetaBean);
            when(gsiIndexMetaBean.getIndexColumns()).thenReturn(new ArrayList<>());
            when(gsiIndexMetaBean.getCoveringColumns()).thenReturn(new ArrayList<>());
            when(schemaManager.getTddlRuleManager()).thenReturn(tddlRuleManager);
            when(tddlRuleManager.getTableGroupInfoManager()).thenReturn(tableGroupInfoManager);
            when(tableGroupInfoManager.getTableGroupConfigById(any())).thenReturn(tableGroupConfig);
            when(tableGroupConfig.getTableGroupRecord()).thenReturn(tableGroupRecord);
            when(tableGroupRecord.getTg_name()).thenReturn("tg12345");
            when(tableGroupConfig.getTables()).thenReturn(ImmutableList.of("t1"));

            mockedAlterRepartitionUtils.when(
                    () -> AlterRepartitionUtils.generateSqlPartitionBy(any(), any(), any(), any()))
                .thenReturn(sqlPartitionBy);

            mockedAlterRepartitionUtils.when(
                    () -> AlterRepartitionUtils.initIndexInfo(anyString(), anyList(), anyList(), anyBoolean(), anyBoolean(),
                        any(), any(), any(SqlPartitionBy.class), any(), anyBoolean()))
                .thenReturn(sqlIndexDefinition);

            mockedTddlSqlToRelConverter.when(() -> TddlSqlToRelConverter.unwrapGsiName(any())).thenReturn("g1");

            mockedSqlConverter.when(() -> SqlConverter.getInstance(any(), any())).thenReturn(sqlConverter);
            when(sqlConverter.getRexInfoFromPartition(any(), any())).thenReturn(new HashMap<>());
            mockedPlannerContext.when(() -> PlannerContext.getPlannerContext(any(), any()))
                .thenReturn(mock(PlannerContext.class));
            when(partitionInfo.getPartitionBy()).thenReturn(partitionByDefinition);
            when(partitionByDefinition.getStrategy()).thenReturn(PartitionStrategy.KEY);
            baseDdlOperation.relDdl = alterTable;
            when(alterTable.getAllRexExprInfo()).thenReturn(new HashMap<>());

            when(alterTableWithGsiPreparedData.getGlobalIndexPreparedData()).thenReturn(
                ImmutableList.of(mock(AlterTablePreparedData.class)));

            Map<String, String> indexTableGroupMap1 = new HashMap<>();
            indexTableGroupMap1.put("g1", "tgi1");
            Map<String, String> indexTableGroupMap2 = new HashMap<>();
            indexTableGroupMap2.put("g2", "tgi2");
            when(sqlAlterTable.getIndexTableGroupMap()).thenReturn(indexTableGroupMap1).thenReturn(indexTableGroupMap1)
                .thenReturn(indexTableGroupMap2).thenReturn(indexTableGroupMap2);

            when(rebuildTablePrepareData.getNeedReHash()).thenCallRealMethod();

            // 调用方法并验证结果
            List<SqlIndexDefinition> sqlIndexDefinitions =
                logicalAlterTableHandler.buildIndexDefinition4AutoForTest("d1", "t1", baseDdlOperation,
                    executionContext,
                    alterTableWithGsiPreparedData, alterTablePreparedData,
                    rebuildTablePrepareData, mock(AtomicBoolean.class), mock(List.class), mock(Pair.class),
                    sqlAlterTable);

            Assert.assertTrue(rebuildTablePrepareData.getNeedReHash().values().contains(true));
            Assert.assertTrue(sqlIndexDefinitions.size() == 2);

            rebuildTablePrepareData.getNeedReHash().clear();
            try {
                sqlIndexDefinitions =
                    logicalAlterTableHandler.buildIndexDefinition4AutoForTest("d1", "t1", baseDdlOperation,
                        executionContext,
                        alterTableWithGsiPreparedData, alterTablePreparedData,
                        rebuildTablePrepareData, mock(AtomicBoolean.class), mock(List.class), mock(Pair.class),
                        sqlAlterTable);
            } catch (TddlRuntimeException ex) {
                Assert.assertFalse(rebuildTablePrepareData.getNeedReHash().values().contains(true));
            }

            GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean1 =
                new GsiMetaManager.GsiIndexMetaBean(null, "d1", "t1", true, "d1", "t1",
                    null, null, null, null, null, null, null,
                    IndexStatus.PUBLIC, 1, true, false, IndexVisibility.VISIBLE);
            Assert.assertTrue(gsiIndexMetaBean1.getIndexColumns() == null);
            Assert.assertTrue(gsiIndexMetaBean1.getCoveringColumns() == null);
        }

    }

    // 更多测试用例...
}

