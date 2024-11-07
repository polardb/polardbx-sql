package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.IDeltaReadOption;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformerUtil;
import com.alibaba.polardbx.executor.archive.reader.TypeComparison;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelPartitionWise;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SpecifiedOssSplitTest {
    private SpecifiedOssSplit create() {
        String logicalSchema = "testlogicalschema";
        String physicalSchema = "testPhysicalSchema";
        Map<Integer, ParameterContext> params = new HashMap<>();
        String logicalTableName = "testLogicalTableName";
        List<String> phyTableNameList = new ArrayList<>();
        List<String> designatedFile = new ArrayList<>();
        OssSplit.DeltaReadOption deltaReadOption = new SpecifiedOssSplit.DeltaReadWithPositionOption(
            100L, 100L, 100L, 100L
        );
        Long checkpointTso = 100L;
        int partIndex = 0;
        Boolean localPairWise = false;
        return new SpecifiedOssSplit(
            logicalSchema,
            physicalSchema,
            params,
            logicalTableName,
            phyTableNameList,
            designatedFile,
            deltaReadOption,
            checkpointTso,
            partIndex,
            localPairWise
        );
    }

    @Test
    public void testCreateAndCopy() {
        SpecifiedOssSplit specifiedOssSplit = create();
        SpecifiedOssSplit.DeltaReadWithPositionOption deltaReadOption =
            (SpecifiedOssSplit.DeltaReadWithPositionOption) specifiedOssSplit.getDeltaReadOption();
        SpecifiedOssSplit.DeltaReadWithPositionOption copyDeltaReadOption =
            new SpecifiedOssSplit.DeltaReadWithPositionOption(
                deltaReadOption.getCheckpointTso(),
                deltaReadOption.getAllCsvFiles(),
                deltaReadOption.getAllPositions(),
                deltaReadOption.getAllDelPositions(),
                deltaReadOption.getProjectColumnIndexes(),
                deltaReadOption.getTsoV0(),
                deltaReadOption.getTsoV1(),
                deltaReadOption.getTableId(),
                deltaReadOption.getCsvFiles(),
                deltaReadOption.getCsvStartPos(),
                deltaReadOption.getCsvEndPos(),
                deltaReadOption.getDelFiles(),
                deltaReadOption.getDelBeginPos(),
                deltaReadOption.getDelEndPos()
            );
        Assert.assertEquals(deltaReadOption.getCheckpointTso(), copyDeltaReadOption.getCheckpointTso());
        Assert.assertEquals(deltaReadOption.getAllCsvFiles(), copyDeltaReadOption.getAllCsvFiles());
        Assert.assertEquals(deltaReadOption.getAllPositions(), copyDeltaReadOption.getAllPositions());
        Assert.assertEquals(deltaReadOption.getAllDelPositions(), copyDeltaReadOption.getAllDelPositions());
        Assert.assertEquals(deltaReadOption.getProjectColumnIndexes(), copyDeltaReadOption.getProjectColumnIndexes());
        Assert.assertEquals(deltaReadOption.getTsoV0(), copyDeltaReadOption.getTsoV0());
        Assert.assertEquals(deltaReadOption.getTsoV1(), copyDeltaReadOption.getTsoV1());
        Assert.assertEquals(deltaReadOption.getTableId(), copyDeltaReadOption.getTableId());
        Assert.assertEquals(deltaReadOption.getCsvFiles(), copyDeltaReadOption.getCsvFiles());
        Assert.assertEquals(deltaReadOption.getCsvStartPos(), copyDeltaReadOption.getCsvStartPos());
        Assert.assertEquals(deltaReadOption.getCsvEndPos(), copyDeltaReadOption.getCsvEndPos());
        Assert.assertEquals(deltaReadOption.getDelFiles(), copyDeltaReadOption.getDelFiles());
        Assert.assertEquals(deltaReadOption.getDelBeginPos(), copyDeltaReadOption.getDelBeginPos());
        Assert.assertEquals(deltaReadOption.getDelEndPos(), copyDeltaReadOption.getDelEndPos());

        copyDeltaReadOption.setDelBeginPos(deltaReadOption.getDelBeginPos());
        copyDeltaReadOption.setDelEndPos(deltaReadOption.getDelEndPos());
        copyDeltaReadOption.setDelFiles(deltaReadOption.getDelFiles());
        copyDeltaReadOption.setCsvStartPos(deltaReadOption.getCsvStartPos());
        copyDeltaReadOption.setCsvEndPos(deltaReadOption.getCsvEndPos());
        copyDeltaReadOption.setCsvFiles(deltaReadOption.getCsvFiles());

        Assert.assertEquals(deltaReadOption.getCsvFiles(), copyDeltaReadOption.getCsvFiles());
        Assert.assertEquals(deltaReadOption.getCsvStartPos(), copyDeltaReadOption.getCsvStartPos());
        Assert.assertEquals(deltaReadOption.getCsvEndPos(), copyDeltaReadOption.getCsvEndPos());
        Assert.assertEquals(deltaReadOption.getDelFiles(), copyDeltaReadOption.getDelFiles());
        Assert.assertEquals(deltaReadOption.getDelBeginPos(), copyDeltaReadOption.getDelBeginPos());
        Assert.assertEquals(deltaReadOption.getDelEndPos(), copyDeltaReadOption.getDelEndPos());

        SpecifiedOssSplit copySpecifiedOssSplit = new SpecifiedOssSplit(
            specifiedOssSplit.getLogicalSchema(),
            specifiedOssSplit.getPhysicalSchema(),
            specifiedOssSplit.getParamsBytes(),
            specifiedOssSplit.getLogicalTableName(),
            specifiedOssSplit.getPhyTableNameList(),
            specifiedOssSplit.getDesignatedFile(),
            (SpecifiedOssSplit.DeltaReadWithPositionOption) specifiedOssSplit.getDeltaReadOption(),
            specifiedOssSplit.getCheckpointTso(),
            specifiedOssSplit.getPartIndex(),
            specifiedOssSplit.getNodePartCount(),
            false
        );
        Assert.assertEquals(specifiedOssSplit.getLogicalSchema(), copySpecifiedOssSplit.getLogicalSchema());
        Assert.assertEquals(specifiedOssSplit.getPhysicalSchema(), copySpecifiedOssSplit.getPhysicalSchema());
        Assert.assertEquals(specifiedOssSplit.getParamsBytes(), copySpecifiedOssSplit.getParamsBytes());
        Assert.assertEquals(specifiedOssSplit.getLogicalTableName(), copySpecifiedOssSplit.getLogicalTableName());
        Assert.assertEquals(specifiedOssSplit.getPhyTableNameList(), copySpecifiedOssSplit.getPhyTableNameList());
        Assert.assertEquals(specifiedOssSplit.getDesignatedFile(), copySpecifiedOssSplit.getDesignatedFile());
        Assert.assertEquals(specifiedOssSplit.getDeltaReadOption(), copySpecifiedOssSplit.getDeltaReadOption());
        Assert.assertEquals(specifiedOssSplit.getCheckpointTso(), copySpecifiedOssSplit.getCheckpointTso());
        Assert.assertEquals(specifiedOssSplit.getPartIndex(), copySpecifiedOssSplit.getPartIndex());
        Assert.assertEquals(specifiedOssSplit.getNodePartCount(), copySpecifiedOssSplit.getNodePartCount());
    }

    @Test
    public void testGetSpecifiedSplit() {
        OSSTableScan ossTableScan = mock(OSSTableScan.class);
        PhyTableOperation phyTableOperation = mock(PhyTableOperation.class);
        ExecutionContext executionContext = new ExecutionContext();

        String schema = "testschema";
        String physicalSchema = "testPhysicalSchema";
        String logicalTableName = "logicalCci";
        String phyTableName = "phyCci";
        List<String> phyTableNameList = ImmutableList.of(phyTableName);
        TableMeta tableMeta = mock(TableMeta.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        PhyTableScanBuilder phyTableScanBuilder = mock(PhyTableScanBuilder.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        Map<String, Set<String>> specifiedOrcFiles = ImmutableMap.of(
            "p1", ImmutableSet.of("test1.csv", "test2.csv")
        );
        Map<String, IDeltaReadOption> readDeltaFiles = ImmutableMap.of(
            "p1", new SpecifiedOssSplit.DeltaReadWithPositionOption(100L, 100L, 100L, 100L)
        );
        RelTraitSet relTraitSet = mock(RelTraitSet.class);
        RelPartitionWise partitionWise = mock(RelPartitionWise.class);

        when(phyTableOperation.getSchemaName()).thenReturn(schema);
        when(phyTableOperation.getDbIndex()).thenReturn(physicalSchema);
        when(phyTableOperation.getLogicalTableNames()).thenReturn(ImmutableList.of(logicalTableName));
        when(phyTableOperation.getTableNames()).thenReturn(ImmutableList.of(phyTableNameList));
        executionContext.setSchemaManager(schema, schemaManager);
        when(schemaManager.getTable(logicalTableName)).thenReturn(tableMeta);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyTableScanBuilder);
        when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);
        when(partitionInfo.getPartitionNameByPhyLocation(physicalSchema, phyTableName)).thenReturn("p1");
        executionContext.setReadOrcFiles(specifiedOrcFiles);
        executionContext.setReadDeltaFiles(readDeltaFiles);
        when(ossTableScan.getTraitSet()).thenReturn(relTraitSet);
        when(relTraitSet.getPartitionWise()).thenReturn(partitionWise);
        when(partitionWise.isRemotePartition()).thenReturn(false);

        List<OssSplit> specifiedOssSplits =
            SpecifiedOssSplit.getSpecifiedSplit(ossTableScan, phyTableOperation, executionContext);
        Assert.assertEquals(1, specifiedOssSplits.size());
        Assert.assertEquals(physicalSchema, specifiedOssSplits.get(0).getPhysicalSchema());
        Assert.assertEquals(schema, specifiedOssSplits.get(0).getLogicalSchema());
        Assert.assertEquals(logicalTableName, specifiedOssSplits.get(0).getLogicalTableName());
        Assert.assertEquals(phyTableNameList, specifiedOssSplits.get(0).getPhyTableNameList());
        Assert.assertEquals(specifiedOrcFiles.get("p1").toArray(),
            specifiedOssSplits.get(0).getDesignatedFile().toArray());
        Assert.assertEquals(readDeltaFiles.get("p1"), specifiedOssSplits.get(0).getDeltaReadOption());
    }

    @Test
    public void testGetColumnTransformer() {
        OSSTableScan ossTableScan = mock(OSSTableScan.class);
        ExecutionContext executionContext = new ExecutionContext();
        String fileName = "test.orc";
        SpecifiedOssSplit specifiedOssSplit = create();

        String schema = specifiedOssSplit.getLogicalSchema();
        String logicalTable = specifiedOssSplit.getLogicalTableName();
        ColumnarManager columnarManager = mock(ColumnarManager.class);
        FileMeta fileMeta = mock(FileMeta.class);
        OrcTableScan orcTableScan = mock(OrcTableScan.class);
        ImmutableList<Integer> inProjects = ImmutableList.of(1);
        ImmutableList<String> inProjectNames = ImmutableList.of("a");
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        ColumnMeta columnMeta = mock(ColumnMeta.class);
        Map<Long, Integer> columnIndexMap = ImmutableMap.of(1L, 0);
        List<ColumnMeta> columnMetas = ImmutableList.of(columnMeta);

        executionContext.setSchemaManager(schema, schemaManager);
        when(schemaManager.getTable(logicalTable)).thenReturn(tableMeta);
        when(columnarManager.fileMetaOf(fileName)).thenReturn(fileMeta);
        when(fileMeta.getLogicalTableName()).thenReturn("100");
        when(fileMeta.getSchemaTs()).thenReturn(100L);
        when(columnarManager.getColumnIndex(100L, 100)).thenReturn(columnIndexMap);
        when(tableMeta.getColumn("a")).thenReturn(columnMeta);
        when(tableMeta.getColumnarFieldId(1)).thenReturn(1L);
        when(fileMeta.getColumnMetas()).thenReturn(columnMetas);
        when(orcTableScan.getInProjects()).thenReturn(inProjects);
        when(orcTableScan.getInputProjectName()).thenReturn(inProjectNames);
        when(ossTableScan.getOrcNode()).thenReturn(orcTableScan);
        when(columnMeta.getName()).thenReturn("a");

        try (MockedStatic<ColumnarManager> staticColumnarManager = mockStatic(ColumnarManager.class);
            MockedStatic<OSSColumnTransformerUtil> staticOssColumnTransformerUtil = mockStatic(
                OSSColumnTransformerUtil.class);) {
            staticColumnarManager.when(ColumnarManager::getInstance).thenReturn(columnarManager);
            staticOssColumnTransformerUtil.when(() -> OSSColumnTransformerUtil.compare(any(), any())).thenReturn(
                TypeComparison.IS_EQUAL_YES
            );
            OSSColumnTransformer transformer =
                specifiedOssSplit.getColumnTransformer(ossTableScan, executionContext, fileName, true);

            System.out.println(transformer);
        }
    }
}
