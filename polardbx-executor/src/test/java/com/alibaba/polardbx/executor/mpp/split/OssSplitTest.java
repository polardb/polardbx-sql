package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.MultiVersionColumnarSchema;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PushDownOpt;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PushDownUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelPartitionWise;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.gms.MultiVersionColumnarSchemaTest.PARTITION_GROUP_RECORDS;
import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OssSplitTest {

    @Mock
    private OSSTableScan ossTableScan;
    @Mock
    private OrcTableScan orcTableScan;
    @Mock
    private PhyTableOperation relNode;
    @Mock
    private PhyTableScanBuilder phyTableScanBuilder;
    @Mock
    private SchemaManager schemaManager;
    @Mock
    private TableMeta tableMeta;
    @Mock
    private PartitionInfo partitionInfo;
    @Mock
    private RelTraitSet relTraitSet;
    @Mock
    private RelPartitionWise relPartitionWise;
    @Mock
    private ParamManager paramManager;
    @Mock
    private FileMeta fileMeta;

    private SortedMap<Long, PartitionInfo> partitionInfos;

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedStatic<ColumnarTransactionUtils> columnarTransactionUtilsMockedStatic;

    private MockedConstruction<ColumnarTableMappingAccessor> tableMappingAccessorCtor;
    private MockedConstruction<FilesAccessor> filesAccessorCtor;
    private MockedConstruction<ColumnarAppendedFilesAccessor> cafAccessorCtor;
    private MockedConstruction<PartitionGroupAccessor> partitionGroupAccessorMockedConstruction;
    private MockedConstruction<MultiVersionColumnarSchema> csMockedConstruction;

    private AutoCloseable autoCloseable;

    private static final List<ColumnarAppendedFilesRecord> CAF_RECORDS =
        ImmutableList.of(buildCafRecord("1.csv", "p0", 100L, 100L),
            buildCafRecord("1.del", "p0", 200L, 100L));

    private static final List<FilesRecordSimplified> FILES_RECORDS =
        ImmutableList.of(buildFileRecord("1.orc", "p0", 1L, 3L, 1L),
            buildFileRecord("1.csv", "p0", 1L, 3L, 1L),
            buildFileRecord("1.del", "p0", 1L, 3L, 1L),
            buildFileRecord("WRONG.orc", "p1", 1L, 3L, 1L));

    private static final List<ColumnMeta> COLUMN_METAS =
        ImmutableList.of(new ColumnMeta("1", "id", null,
            new Field("1", "id", TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));

    private static final List<TablePartitionRecord> PARTITION_RECORDS =
        ImmutableList.of(ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 10, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": -1, \"partDesc\": \"\", \"partExpr\": \"\", \"partName\": \"\", \"phyTable\": \"\", \"nextLevel\": 1, \"partFlags\": 0, \"partLevel\": 0, \"tableName\": \"cci1_$39a1\", \"partEngine\": \"\", \"partExtras\": \"{\\\"charset\\\":\\\"utf8\\\",\\\"locality\\\":\\\"\\\",\\\"partitionPattern\\\":\\\"cci1_$39a1_2zTE\\\",\\\"timeZone\\\":\\\"SYSTEM\\\"}\", \"partMethod\": \"\", \"partStatus\": 1, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"db1\", \"partPosition\": -1, \"partTempName\": \"\"}"),
            ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 29, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": 44, \"partDesc\": \"10000\", \"partExpr\": \"`id`\", \"partName\": \"p0\", \"phyTable\": \"cci1_$39a1_2zTE_00000\", \"nextLevel\": -1, \"partFlags\": 0, \"partLevel\": 1, \"tableName\": \"cci1_$39a1\", \"partEngine\": \"Columnar\", \"partExtras\": \"{\\\"locality\\\":\\\"\\\"}\", \"partMethod\": \"RANGE\", \"partStatus\": 0, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"db1\", \"partPosition\": 1, \"partTempName\": \"\"}"),
            ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 30, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": 44, \"partDesc\": \"20000\", \"partExpr\": \"`id`\", \"partName\": \"p1\", \"phyTable\": \"cci1_$39a1_2zTE_00001\", \"nextLevel\": -1, \"partFlags\": 0, \"partLevel\": 1, \"tableName\": \"cci1_$39a1\", \"partEngine\": \"Columnar\", \"partExtras\": \"{\\\"locality\\\":\\\"\\\"}\", \"partMethod\": \"RANGE\", \"partStatus\": 0, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"db1\", \"partPosition\": 2, \"partTempName\": \"\"}"));

    private final ExecutionContext executionContext = new ExecutionContext();

    @Before
    public void setUp() throws Exception {
        autoCloseable = MockitoAnnotations.openMocks(this);
        when(relNode.getSchemaName()).thenReturn("db1");
        when(relNode.getDbIndex()).thenReturn("db1_0");
        when(relNode.getLogicalTableNames()).thenReturn(ImmutableList.of("t1"));
        when(relNode.getTableNames()).thenReturn(ImmutableList.of(ImmutableList.of("t1_0")));
        when(relNode.getPhyOperationBuilder()).thenReturn(phyTableScanBuilder);
        executionContext.setSchemaManager("db1", schemaManager);
        executionContext.setParamManager(paramManager);
        when(schemaManager.getTable(anyString())).thenReturn(tableMeta);

        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(Mockito.mock(Connection.class));
        mockMetaDbUtil.when(() -> MetaDbUtil.queryMetaDbWrapper(Mockito.any(), Mockito.any())).thenCallRealMethod();

        ColumnarTableMappingRecord record = new ColumnarTableMappingRecord("db1", "t1", "cci1_$39a1", 1, "PUBLIC");
        record.tableId = 1;
        tableMappingAccessorCtor = Mockito.mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
            when(mock.querySchemaIndex(anyString(), anyString()))
                .thenReturn(ImmutableList.of(record));
        });

        when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);
        when(partitionInfo.getPartitionNameByPhyLocation(anyString(), anyString())).thenReturn("p0");

        when(ossTableScan.getOrcNode()).thenReturn(orcTableScan);
        when(ossTableScan.getTraitSet()).thenReturn(relTraitSet);
        when(ossTableScan.getSchemaName()).thenReturn("db1");
        when(ossTableScan.getLogicalTableName()).thenReturn("t1");
        when(ossTableScan.isNewPartDbTbl()).thenReturn(true);
        when(relTraitSet.getPartitionWise()).thenReturn(relPartitionWise);
        when(relPartitionWise.isRemotePartition()).thenReturn(false);
        when(relPartitionWise.isLocalPartition()).thenReturn(false);
        when(orcTableScan.getInProjects()).thenReturn(ImmutableList.of());

        partitionGroupAccessorMockedConstruction = Mockito.mockConstruction(
            PartitionGroupAccessor.class,
            (mock, context) -> {
                Mockito.when(mock.getPartitionGroupsByTableGroupId(Mockito.anyLong(), Mockito.anyBoolean()))
                    .thenReturn(Arrays.asList(PARTITION_GROUP_RECORDS));
            });

        ColumnarManager.getInstance().reload();
    }

    public void setUpForFlashback() {
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.isFlashbackQuery()).thenReturn(true);

        columnarTransactionUtilsMockedStatic = Mockito.mockStatic(ColumnarTransactionUtils.class);
        columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(2L);
        columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getMinColumnarSnapshotTime)
            .thenReturn(1L);

        filesAccessorCtor = Mockito.mockConstruction(FilesAccessor.class, (mock, context) -> {
            when(mock.queryColumnarSnapshotFilesByTsoAndTableId(anyLong(), anyString(), anyString()))
                .thenReturn(FILES_RECORDS);
        });

        cafAccessorCtor = Mockito.mockConstruction(ColumnarAppendedFilesAccessor.class, (mock, context) -> {
            when(mock.queryLastValidAppendByTsoAndTableId(anyLong(), anyString(), anyString()))
                .thenReturn(CAF_RECORDS);
        });
    }

    public void setUpForColumnar() {
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.isFlashbackQuery()).thenReturn(false);

        columnarTransactionUtilsMockedStatic = Mockito.mockStatic(ColumnarTransactionUtils.class);
        columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(2L);
        columnarTransactionUtilsMockedStatic.when(ColumnarTransactionUtils::getMinColumnarSnapshotTime)
            .thenReturn(1L);
        filesAccessorCtor = Mockito.mockConstruction(FilesAccessor.class, (mock, context) -> {
            when(mock.queryColumnarDeltaFilesByTsoAndTableId(anyLong(), anyLong(), anyString(), anyString()))
                .thenReturn(FILES_RECORDS);
        });
    }

    public void setUpForSharding() {
        partitionInfos = new ConcurrentSkipListMap<>();
        // TODO(siyun): add more test cases
        partitionInfos.put(1L,
            PartitionInfoManager.generatePartitionInfo(COLUMN_METAS, PARTITION_RECORDS, null, false, false));
        partitionInfos.put(2L,
            PartitionInfoManager.generatePartitionInfo(COLUMN_METAS, PARTITION_RECORDS, null, false, false));
        csMockedConstruction = Mockito.mockConstruction(MultiVersionColumnarSchema.class, (mock, context) -> {
            when(mock.getPartitionInfos(anyLong(), anyLong())).thenReturn(partitionInfos);
        });

        ColumnarManager.getInstance().reload();

        VolcanoPlanner planner = new VolcanoPlanner(new PlannerContext(executionContext));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        RelOptTable mockTable = mock(RelOptTable.class);
        when(mockTable.getQualifiedName()).thenReturn(ImmutableList.of("db1", "t1"));
        when(ossTableScan.getTable()).thenReturn(mockTable);
        when(ossTableScan.getCluster()).thenReturn(cluster);
        when(tableMeta.getStatus()).thenReturn(TableStatus.PUBLIC);
        doCallRealMethod().when(tableMeta).getRowType(any());
        when(tableMeta.getAllColumns()).thenReturn(COLUMN_METAS);
        when(ossTableScan.getRowType()).thenReturn(CalciteUtils.switchRowType(COLUMN_METAS, typeFactory));
        when(ossTableScan.getTableNames()).thenReturn(ImmutableList.of("t1"));
        PushDownOpt pushDownOpt = new PushDownOpt(ossTableScan, DbType.MYSQL, executionContext);

        final JavaTypeFactoryImpl javaTypeFactory =
            new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(javaTypeFactory);
        final RexLiteral i1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);

        final RelDataType intType = javaTypeFactory.createType(int.class);
        final RexInputRef x = rexBuilder.makeInputRef(intType, 0); // $0

        final RexNode xEq1 =
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, x, i1);

        LogicalFilter filter = new LogicalFilter(cluster, RelTraitSet.createEmpty(), ossTableScan, xEq1,
            ImmutableSet.of());
        PushDownUtils.pushFilter(filter, pushDownOpt.getBuilder());

        when(ossTableScan.getPushDownOpt()).thenReturn(pushDownOpt);

        doCallRealMethod().when(ossTableScan).getCciPartPrunedResults(any(), any());
        doCallRealMethod().when(ossTableScan).getCciRelShardInfo(any(), any());

        when(paramManager.getInt(ConnectionParams.PARTITION_PRUNING_STEP_COUNT_LIMIT)).thenReturn(1024);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_PARTITION_PRUNING)).thenReturn(true);
    }

    public void setUpForRawString() {
        Parameters parameters = new Parameters();
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        params.put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {
                1, new RawString(IntStream.range(0, 10000).boxed().collect(
                Collectors.toList()))}));
        parameters.setParams(params);
        executionContext.setParams(parameters);
        when(paramManager.getInt(ConnectionParams.IN_PRUNE_MAX_TIME)).thenReturn(100000);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_PARTITION_PRUNING)).thenReturn(true);

        PartitionInfo tmpPartitionInfo =
            PartitionInfoManager.generatePartitionInfo(COLUMN_METAS, PARTITION_RECORDS, null, false, false);
        when(tableMeta.getPartitionInfo()).thenReturn(tmpPartitionInfo);

        PushDownOpt pushDownOpt = new PushDownOpt(ossTableScan, DbType.MYSQL, executionContext);

        final JavaTypeFactoryImpl javaTypeFactory =
            new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(javaTypeFactory);

        final RelDataType intType = javaTypeFactory.createType(int.class);
        final RexInputRef x = rexBuilder.makeInputRef(intType, 0); // $0

        final RexNode xinList =
            rexBuilder.makeCall(TddlOperatorTable.IN, x,
                rexBuilder.makeCall(SqlStdOperatorTable.ROW,
                    rexBuilder.makeDynamicParam(intType, 0)));

        LogicalFilter filter =
            new LogicalFilter(ossTableScan.getCluster(), RelTraitSet.createEmpty(), ossTableScan, xinList,
                ImmutableSet.of());
        PushDownUtils.pushFilter(filter, pushDownOpt.getBuilder());

        when(ossTableScan.getPushDownOpt()).thenReturn(pushDownOpt);
    }

    public void setUpForColdData() {
        when(ossTableScan.isColumnarIndex()).thenReturn(false);

        when(fileMeta.getFileName()).thenReturn("1.orc");
        when(fileMeta.getCommitTs()).thenReturn(1L);
        Map<String, List<FileMeta>> flatFileMetas = new HashMap<>();
        flatFileMetas.put("t1_0", ImmutableList.of(fileMeta));
        when(tableMeta.getFlatFileMetas()).thenReturn(flatFileMetas);

        when(paramManager.getString(ConnectionParams.FILE_LIST)).thenReturn("ALL");
    }

    @After
    public void clear() throws Exception {
        if (autoCloseable != null) {
            autoCloseable.close();
        }

        if (columnarTransactionUtilsMockedStatic != null) {
            columnarTransactionUtilsMockedStatic.close();
        }

        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }

        if (tableMappingAccessorCtor != null) {
            tableMappingAccessorCtor.close();
        }

        if (filesAccessorCtor != null) {
            filesAccessorCtor.close();
        }

        if (cafAccessorCtor != null) {
            cafAccessorCtor.close();
        }

        if (partitionGroupAccessorMockedConstruction != null) {
            partitionGroupAccessorMockedConstruction.close();
        }

        if (csMockedConstruction != null) {
            csMockedConstruction.close();
        }
    }

    @Test
    public void getTableConcurrencySplitForFlashback() {
        setUpForFlashback();
        List<OssSplit> splits = OssSplit.getTableConcurrencySplit(ossTableScan, relNode, executionContext, 1L);
        assertEquals(1, splits.size());
        OssSplit split = splits.get(0);
        assertEquals("1.orc", split.getDesignatedFile().get(0));
        List<String> csvFiles = split.getDeltaReadOption().getAllCsvFiles().get("t1_0");
        List<Long> positions = split.getDeltaReadOption().getAllPositions().get("t1_0");
        assertEquals(1, csvFiles.size());
        assertEquals("1.csv", csvFiles.get(0));
        assertEquals(1, positions.size());
        assertEquals(200, positions.get(0).longValue());
        List<Pair<String, Long>> delPositions = split.getDeltaReadOption().getAllDelPositions().get("p0");
        assertEquals(1, delPositions.size());
        assertEquals("1.del", delPositions.get(0).getKey());
        assertEquals(300, delPositions.get(0).getValue().longValue());
    }

    @Test
    public void getFileConcurrencySplitForFlashback() {
        setUpForFlashback();
        List<OssSplit> splits = OssSplit.getFileConcurrencySplit(ossTableScan, relNode, executionContext, 1L);
        assertEquals(2, splits.size());
        for (OssSplit split : splits) {
            if (split.getDesignatedFile() != null && split.getDesignatedFile().size() == 1) {
                assertEquals("1.orc", split.getDesignatedFile().get(0));
            } else {
                List<String> csvFiles = split.getDeltaReadOption().getAllCsvFiles().get("t1_0");
                List<Long> positions = split.getDeltaReadOption().getAllPositions().get("t1_0");
                assertEquals(1, csvFiles.size());
                assertEquals("1.csv", csvFiles.get(0));
                assertEquals(1, positions.size());
                assertEquals(200, positions.get(0).longValue());
            }
            List<Pair<String, Long>> delPositions = split.getDeltaReadOption().getAllDelPositions().get("p0");
            assertEquals(1, delPositions.size());
            assertEquals("1.del", delPositions.get(0).getKey());
            assertEquals(300, delPositions.get(0).getValue().longValue());
        }
    }

    @Test
    public void getFileConcurrencySplitForColumnar() {
        setUpForColumnar();
        List<OssSplit> splits = OssSplit.getFileConcurrencySplit(ossTableScan, relNode, executionContext, 1L);
        assertEquals(2, splits.size());
        for (OssSplit split : splits) {
            if (split.getDesignatedFile() != null && split.getDesignatedFile().size() == 1) {
                assertEquals("1.orc", split.getDesignatedFile().get(0));
            } else {
                List<String> csvFiles = split.getDeltaReadOption().getAllCsvFiles().get("t1_0");
                assertEquals(1, csvFiles.size());
                assertEquals("1.csv", csvFiles.get(0));
            }
        }
    }

    @Test
    public void getFileConcurrencySplitForColdData() {
        setUpForColdData();
        List<OssSplit> splits = OssSplit.getFileConcurrencySplit(ossTableScan, relNode, executionContext, null);
        assertEquals(1, splits.size());
        for (OssSplit split : splits) {
            assertEquals("1.orc", split.getDesignatedFile().get(0));
            List<String> prunedOrcFiles = split.getPrunedOrcFiles(ossTableScan, executionContext);
            assertEquals(0, prunedOrcFiles.size());
        }
    }

    @Test
    public void getColumnarSplitForFlashbackQuery() {
        setUpForFlashback();
        setUpForSharding();

        SplitInfo splitInfo = SplitManagerImpl.columnarOssTableScanSplit(ossTableScan, executionContext, 1L);
        Collection<List<Split>> splits = splitInfo.getSplits();
        assertEquals(1, splits.size());
        List<Split> splitList = splits.stream().findFirst().get();
        assertEquals(2, splitList.size());
        for (Split wrapSplit : splitList) {
            OssSplit split = (OssSplit) wrapSplit.getConnectorSplit();
            if (split.getDesignatedFile() != null && split.getDesignatedFile().size() == 1) {
                assertEquals("1.orc", split.getDesignatedFile().get(0));
            } else {
                List<String> csvFiles = split.getDeltaReadOption().getAllCsvFiles().get("p0");
                List<Long> positions = split.getDeltaReadOption().getAllPositions().get("p0");
                assertEquals(1, csvFiles.size());
                assertEquals("1.csv", csvFiles.get(0));
                assertEquals(1, positions.size());
                assertEquals(200, positions.get(0).longValue());
            }
            List<Pair<String, Long>> delPositions = split.getDeltaReadOption().getAllDelPositions().get("p0");
            assertEquals(1, delPositions.size());
            assertEquals("1.del", delPositions.get(0).getKey());
            assertEquals(300, delPositions.get(0).getValue().longValue());
        }
    }

    @Test
    public void getColumnarSplitForColumnarQuery() {
        setUpForColumnar();
        setUpForSharding();

        SplitInfo splitInfo = SplitManagerImpl.columnarOssTableScanSplit(ossTableScan, executionContext, 1L);
        Collection<List<Split>> splits = splitInfo.getSplits();
        assertEquals(1, splits.size());
        List<Split> splitList = splits.stream().findFirst().get();
        assertEquals(2, splitList.size());
        for (Split wrapSplit : splitList) {
            OssSplit split = (OssSplit) wrapSplit.getConnectorSplit();
            if (split.getDesignatedFile() != null && split.getDesignatedFile().size() == 1) {
                assertEquals("1.orc", split.getDesignatedFile().get(0));
            } else {
                List<String> csvFiles = split.getDeltaReadOption().getAllCsvFiles().get("p0");
                assertEquals(1, csvFiles.size());
                assertEquals("1.csv", csvFiles.get(0));
            }
        }
    }

    @Test
    public void getColumnarSplitForColumnarQueryWithRawString() {
        setUpForColumnar();
        setUpForSharding();
        setUpForRawString();

        SplitInfo splitInfo = SplitManagerImpl.columnarOssTableScanSplit(ossTableScan, executionContext, 1L);
        Collection<List<Split>> splits = splitInfo.getSplits();
        assertEquals(1, splits.size());
        List<Split> splitList = splits.stream().findFirst().get();
        assertEquals(2, splitList.size());
        for (Split wrapSplit : splitList) {
            OssSplit split = (OssSplit) wrapSplit.getConnectorSplit();
            if (split.getDesignatedFile() != null && split.getDesignatedFile().size() == 1) {
                assertEquals("1.orc", split.getDesignatedFile().get(0));
            } else {
                List<String> csvFiles = split.getDeltaReadOption().getAllCsvFiles().get("p0");
                assertEquals(1, csvFiles.size());
                assertEquals("1.csv", csvFiles.get(0));
            }
        }
    }

    @Ignore("Used for perf")
    @Test
    public void perfMultiVersionPruning() {
        setUpForColumnar();
        setUpForSharding();
        setUpForRawString();

        for (int i = 0; i < 10; i++) {
            OptimizerUtils.pruningInValueForColumnar(ossTableScan, executionContext, partitionInfos);
        }
        long avg = executionContext.getPruningTime() / 10;
        System.out.println("avg pruning time(ms):" + avg);
    }

    static ColumnarAppendedFilesRecord buildCafRecord(String fileName, String partitionName, long appendOffset,
                                                      long appendLength) {
        ColumnarAppendedFilesRecord cafRecord = new ColumnarAppendedFilesRecord();
        cafRecord.fileName = fileName;
        cafRecord.partName = partitionName;
        cafRecord.appendOffset = appendOffset;
        cafRecord.appendLength = appendLength;
        return cafRecord;
    }

    static FilesRecordSimplified buildFileRecord(String fileName, String partitionName, long commitTs, long removeTs,
                                                 long schemaTs) {
        FilesRecordSimplified fileRecord = new FilesRecordSimplified();
        fileRecord.fileName = fileName;
        fileRecord.partitionName = partitionName;
        fileRecord.commitTs = commitTs;
        fileRecord.removeTs = removeTs;
        fileRecord.schemaTs = schemaTs;
        return fileRecord;
    }
}