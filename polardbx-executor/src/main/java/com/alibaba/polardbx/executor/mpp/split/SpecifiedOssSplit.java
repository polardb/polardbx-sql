package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.IDeltaReadOption;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitionWise;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;
import static com.alibaba.polardbx.optimizer.utils.PartitionUtils.calcPartition;

/**
 * This oss split contains specified orc/csv/del files.
 * It will also read TSO column.
 *
 * @author yaozhili
 */
public class SpecifiedOssSplit extends OssSplit {
    public SpecifiedOssSplit(String logicalSchema, String physicalSchema,
                             Map<Integer, ParameterContext> params,
                             String logicalTableName, List<String> phyTableNameList,
                             List<String> designatedFile, DeltaReadOption deltaReadOption, Long checkpointTso,
                             int partIndex, Boolean localPairWise) {
        super(logicalSchema, physicalSchema, params, logicalTableName, phyTableNameList, designatedFile,
            deltaReadOption,
            checkpointTso, partIndex, localPairWise);
    }

    @JsonCreator
    public SpecifiedOssSplit(
        @JsonProperty("logicalSchema") String logicalSchema,
        @JsonProperty("physicalSchema") String physicalSchema,
        @JsonProperty("paramsBytes") byte[] paramsBytes,
        @JsonProperty("logicalTableName") String logicalTableName,
        @JsonProperty("phyTableNameList") List<String> phyTableNameList,
        @JsonProperty("designatedFile") List<String> designatedFile,
        @JsonProperty("deltaReadOption") DeltaReadWithPositionOption deltaReadOption,
        @JsonProperty("checkpointTso") Long checkpointTso,
        @JsonProperty("partIndex") int partIndex,
        @JsonProperty("nodePartCount") int nodePartCount,
        @JsonProperty("localPairWise") boolean localPairWise) {
        super(logicalSchema, physicalSchema, paramsBytes, logicalTableName, phyTableNameList, designatedFile,
            deltaReadOption, checkpointTso, partIndex, nodePartCount, localPairWise);
    }

    public static List<OssSplit> getSpecifiedSplit(OSSTableScan ossTableScan, RelNode relNode,
                                                   ExecutionContext executionContext) {
        Preconditions.checkArgument(relNode instanceof PhyTableOperation);
        List<OssSplit> splits = new ArrayList<>();

        PhyTableOperation phyTableOperation = (PhyTableOperation) relNode;
        String logicalSchema = phyTableOperation.getSchemaName();
        String physicalSchema = phyTableOperation.getDbIndex();

        String logicalTableName = phyTableOperation.getLogicalTableNames().get(0);
        List<String> phyTableNameList = phyTableOperation.getTableNames().get(0);

        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTableName);

        PhyTableScanBuilder phyOperationBuilder =
            (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();

        final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        Map<String, Set<String>> specifiedOrcFiles = executionContext.getReadOrcFiles();
        Map<String, IDeltaReadOption> specifiedDeltaFiles = executionContext.getReadDeltaFiles();

        // for each physical table
        for (int i = 0; i < phyTableNameList.size(); i++) {
            String phyTable = phyTableNameList.get(i);

            // Find part name from physical schema + physical table
            final String partName = partitionInfo.getPartitionNameByPhyLocation(physicalSchema, phyTable);

            Set<String> orcFiles = null == specifiedOrcFiles ? null : specifiedOrcFiles.get(partName);
            DeltaReadWithPositionOption deltaReadOption = null == specifiedDeltaFiles ? null :
                (DeltaReadWithPositionOption) specifiedDeltaFiles.get(partName);

            if (null == orcFiles
                && (executionContext.isReadOrcOnly()
                || null == deltaReadOption
                || null == deltaReadOption.getCsvFiles())) {
                // case 1: no orc files but only orc files are considered.
                // case 2: no orc files and no delta files.
                // case 3: no orc files and no csv files, only delete files mean nothing.
                continue;
            }

            RelPartitionWise partitionWise = ossTableScan.getTraitSet().getPartitionWise();
            boolean needPartition = partitionWise.isRemotePartition() || executionContext.getParamManager()
                .getBoolean(ConnectionParams.SCHEDULE_BY_PARTITION);

            int partition = needPartition ?
                calcPartition(logicalSchema, logicalTableName, physicalSchema, phyTable) : NO_PARTITION_INFO;
            boolean localPairWise =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                    && partitionWise.isLocalPartition();

            List<String> singlePhyTableNameList = ImmutableList.of(phyTable);

            Map<Integer, ParameterContext> params =
                phyOperationBuilder.buildSplitParamMap(singlePhyTableNameList);

            OssSplit ossSplit = new SpecifiedOssSplit(
                logicalSchema, physicalSchema, params,
                logicalTableName, singlePhyTableNameList,
                null == orcFiles ? null : new ArrayList<>(orcFiles), deltaReadOption,
                null == deltaReadOption ? 0 : deltaReadOption.getCheckpointTso(), partition, localPairWise);
            splits.add(ossSplit);
        }
        return splits;
    }

    public OSSColumnTransformer getColumnTransformer(OSSTableScan ossTableScan,
                                                     ExecutionContext executionContext,
                                                     String fileName,
                                                     boolean addTsoInfo) {
        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTableName);
        List<Timestamp> timestamps = new ArrayList<>();
        List<ColumnMeta> columnMetas = new ArrayList<>();
        List<ColumnMeta> fileColumnMetas = new ArrayList<>();
        List<ColumnMeta> initColumnMetas = new ArrayList<>();
        List<Integer> locInOrc = new ArrayList<>();

        FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(fileName);
        List<Integer> inProjects = ossTableScan.getOrcNode().getInProjects();
        List<String> inProjectNames = ossTableScan.getOrcNode().getInputProjectName();
        long tableId = Long.parseLong(fileMeta.getLogicalTableName());

        Map<Long, Integer> columnIndexMap =
            ColumnarManager.getInstance().getColumnIndex(fileMeta.getSchemaTs(), tableId);

        for (int i = 0; i < inProjects.size(); i++) {
            Integer columnIndex = inProjects.get(i);
            String columnName = inProjectNames.get(i);

            columnMetas.add(tableMeta.getColumn(columnName));

            long fieldId = tableMeta.getColumnarFieldId(columnIndex);
            Integer actualColumnIndex = columnIndexMap.get(fieldId);
            if (actualColumnIndex != null) {
                fileColumnMetas.add(fileMeta.getColumnMetas().get(actualColumnIndex));
                locInOrc.add(actualColumnIndex + 1);
                timestamps.add(null);
                initColumnMetas.add(null);
            } else {
                ColumnMetaWithTs metaWithTs = ColumnarManager.getInstance().getInitColumnMeta(tableId, fieldId);
                fileColumnMetas.add(null);
                locInOrc.add(null);
                timestamps.add(metaWithTs.getCreate());
                initColumnMetas.add(metaWithTs.getMeta());
            }
        }

        // Add TSO column meta to the last column.
        if (addTsoInfo) {
            ColumnMeta tsoColumnMeta = new ColumnMeta(tableMeta.getTableName(), "tso", null,
                new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
            columnMetas.add(tsoColumnMeta);
            fileColumnMetas.add(fileMeta.getColumnMetas().get(0));
            locInOrc.add(1);
        }

        return new OSSColumnTransformer(
            columnMetas,
            fileColumnMetas,
            initColumnMetas,
            timestamps,
            locInOrc
        );
    }

    public static class DeltaReadWithPositionOption extends DeltaReadOption {
        private final long tsoV0;
        private final long tsoV1;
        private final long tableId;
        private List<String> csvFiles;
        private List<Long> csvStartPos;
        private List<Long> csvEndPos;
        private List<String> delFiles;
        private List<Long> delBeginPos;
        private List<Long> delEndPos;

        public DeltaReadWithPositionOption(long checkpointTso, long tsoV0, long tsoV1, long tableId) {
            super(checkpointTso);
            this.tsoV0 = tsoV0;
            this.tsoV1 = tsoV1;
            this.tableId = tableId;
        }

        @JsonCreator
        public DeltaReadWithPositionOption(
            @JsonProperty("checkpointTso") long checkpointTso,
            @JsonProperty("allCsvFiles") Map<String, List<String>> allCsvFiles,
            @JsonProperty("allPositions") Map<String, List<Long>> allPositions,
            @JsonProperty("allDelPositions") Map<String, List<Pair<String, Long>>> allDelPositions,
            @JsonProperty("projectColumnIndexes") List<Integer> projectColumnIndexes,
            @JsonProperty("tsoV0") long tsoV0,
            @JsonProperty("tsoV1") long tsoV1,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("csvFiles") List<String> csvFiles,
            @JsonProperty("csvStartPos") List<Long> startPos,
            @JsonProperty("csvEndPos") List<Long> endPos,
            @JsonProperty("delFiles") List<String> delFiles,
            @JsonProperty("delBeginPos") List<Long> delBeginPos,
            @JsonProperty("delEndPos") List<Long> delEndPos) {
            super(checkpointTso, allCsvFiles, allPositions, allDelPositions, projectColumnIndexes);
            this.tsoV0 = tsoV0;
            this.tsoV1 = tsoV1;
            this.tableId = tableId;
            this.csvFiles = csvFiles;
            this.csvStartPos = startPos;
            this.csvEndPos = endPos;
            this.delFiles = delFiles;
            this.delBeginPos = delBeginPos;
            this.delEndPos = delEndPos;
        }

        @JsonProperty
        public List<String> getCsvFiles() {
            return csvFiles;
        }

        @JsonProperty
        public List<Long> getCsvStartPos() {
            return csvStartPos;
        }

        @JsonProperty
        public List<Long> getCsvEndPos() {
            return csvEndPos;
        }

        @JsonProperty
        public List<String> getDelFiles() {
            return delFiles;
        }

        @JsonProperty
        public List<Long> getDelBeginPos() {
            return delBeginPos;
        }

        @JsonProperty
        public List<Long> getDelEndPos() {
            return delEndPos;
        }

        @JsonProperty
        public long getTsoV0() {
            return tsoV0;
        }

        @JsonProperty
        public long getTsoV1() {
            return tsoV1;
        }

        @JsonProperty
        public long getTableId() {
            return tableId;
        }

        public void setCsvFiles(List<String> csvFiles) {
            this.csvFiles = csvFiles;
        }

        public void setCsvStartPos(List<Long> csvStartPos) {
            this.csvStartPos = csvStartPos;
        }

        public void setCsvEndPos(List<Long> csvEndPos) {
            this.csvEndPos = csvEndPos;
        }

        public void setDelFiles(List<String> delFiles) {
            this.delFiles = delFiles;
        }

        public void setDelBeginPos(List<Long> delBeginPos) {
            this.delBeginPos = delBeginPos;
        }

        public void setDelEndPos(List<Long> delEndPos) {
            this.delEndPos = delEndPos;
        }
    }
}
