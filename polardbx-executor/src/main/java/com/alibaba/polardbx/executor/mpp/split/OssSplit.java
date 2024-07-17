/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.SerializeUtils;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.executor.archive.predicate.OSSPredicateBuilder;
import com.alibaba.polardbx.executor.archive.pruning.AggPruningResult;
import com.alibaba.polardbx.executor.archive.pruning.OrcFilePruningResult;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.reader.OSSReadOption;
import com.alibaba.polardbx.executor.archive.reader.TypeComparison;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.executor.archive.schemaevolution.OrcColumnManager;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarStoreUtils;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.partition.PartSpecBase;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitionWise;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.SearchArgument;
import org.apache.orc.sarg.SearchArgumentFactory;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import static com.alibaba.polardbx.optimizer.utils.ITimestampOracle.BITS_LOGICAL_TIME;
import static com.google.common.base.MoreObjects.toStringHelper;

public class OssSplit implements ConnectorSplit {
    public static final Integer NO_PARTITION_INFO = -1;

    private List<OSSReadOption> readOptions;

    private String logicalSchema;
    private String physicalSchema;

    // all physical tables in a logical table share the parameters
    private Map<Integer, ParameterContext> params;

    private TypeDescription readSchema;

    // search argument / columns
    // all files in a ossSplit share the search arguments / columns.
    private SearchArgument searchArgument;
    private String[] columns;

    private boolean enableAggPruner;

    private String logicalTableName;
    private List<String> phyTableNameList;

    // all fileMetas share the same version of schema
    private List<List<FileMeta>> allFileMetas;
    private List<String> designatedFile;
    private byte[] paramsBytes;

    private boolean isInit = false;

    // for delta of columnar store (nullable)
    private DeltaReadOption deltaReadOption;
    private Long checkpointTso;

    private int partIndex = -1;

    private int nodePartCount = -1;

    private Boolean localPairWise;

    public OssSplit(String logicalSchema, String physicalSchema,
                    Map<Integer, ParameterContext> params,
                    String logicalTableName,
                    List<String> phyTableNameList,
                    List<String> designatedFile,
                    DeltaReadOption deltaReadOption,
                    Long checkpointTso,
                    int partIndex,
                    Boolean localPairWise) {
        this.logicalSchema = logicalSchema;
        this.physicalSchema = physicalSchema;
        this.params = params;
        this.logicalTableName = logicalTableName;
        this.phyTableNameList = phyTableNameList;
        this.designatedFile = designatedFile;
        this.deltaReadOption = deltaReadOption;
        this.checkpointTso = checkpointTso;
        this.partIndex = partIndex;
        this.localPairWise = localPairWise;
    }

    @JsonCreator
    public OssSplit(
        @JsonProperty("logicalSchema") String logicalSchema,
        @JsonProperty("physicalSchema") String physicalSchema,
        @JsonProperty("paramsBytes") byte[] paramsBytes,
        @JsonProperty("logicalTableName") String logicalTableName,
        @JsonProperty("phyTableNameList") List<String> phyTableNameList,
        @JsonProperty("designatedFile") List<String> designatedFile,
        @JsonProperty("deltaReadOption") DeltaReadOption deltaReadOption,
        @JsonProperty("checkpointTso") Long checkpointTso,
        @JsonProperty("partIndex") int partIndex,
        @JsonProperty("nodePartCount") int nodePartCount,
        @JsonProperty("localPairWise") boolean localPairWise) {
        this.logicalSchema = logicalSchema;
        this.physicalSchema = physicalSchema;
        this.paramsBytes = paramsBytes;
        this.logicalTableName = logicalTableName;
        this.phyTableNameList = phyTableNameList;
        this.designatedFile = designatedFile;
        this.partIndex = partIndex;
        this.nodePartCount = nodePartCount;
        this.deltaReadOption = deltaReadOption;
        this.checkpointTso = checkpointTso;
        this.localPairWise = localPairWise;
    }

    @JsonProperty
    public DeltaReadOption getDeltaReadOption() {
        return deltaReadOption;
    }

    @JsonProperty
    public Long getCheckpointTso() {
        return checkpointTso;
    }

    @Override
    @JsonIgnore
    public String getHostAddress() {
        return null;
    }

    @Override
    @JsonIgnore
    public Object getInfo() {
        return null;
    }

    @JsonProperty
    public int getPartIndex() {
        return partIndex;
    }

    public void setPartIndex(int partIndex) {
        this.partIndex = partIndex;
    }

    /**
     * group file meta by version
     *
     * @param relNode the physical operation
     * @return list of OssSplit, each split has the same version of meta
     */
    public static List<OssSplit> getTableConcurrencySplit(OSSTableScan ossTableScan, RelNode relNode,
                                                          ExecutionContext executionContext,
                                                          Long tso) {
        Preconditions.checkArgument(relNode instanceof PhyTableOperation);
        PhyTableOperation phyTableOperation = (PhyTableOperation) relNode;
        String logicalSchema = phyTableOperation.getSchemaName();
        String physicalSchema = phyTableOperation.getDbIndex();
        String logicalTable = phyTableOperation.getLogicalTableNames().get(0);
        List<String> phyTableList = phyTableOperation.getTableNames().get(0);

        PhyTableScanBuilder phyOperationBuilder =
            (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();

        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTable);
        Map<String, List<FileMeta>> flatFileMetas = FileMeta.getFlatFileMetas(tableMeta);

        if (ossTableScan.isColumnarIndex()) {
            DeltaReadOption deltaReadOption = null;

            Map<Integer, ParameterContext> params = phyOperationBuilder.buildSplitParamMap(phyTableList);
            final ColumnarManager columnarManager = ColumnarManager.getInstance();

            // build delta read option
            deltaReadOption = new DeltaReadOption(tso);
            final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

            Map<String, List<String>> allCsvFiles = new HashMap<>();
            List<String> allOrcFiles = new ArrayList<>();
            if (executionContext.isReadOrcOnly()) {
                // Special hint, only read specified orc files.
                // Normal columnar read should not get here.
                allOrcFiles.addAll(executionContext.getReadOrcFiles());
            } else {
                for (String physicalTable : phyTableList) {
                    // Find part name from physical schema + physical table
                    final String partName = partitionInfo.getPartitionNameByPhyLocation(physicalSchema, physicalTable);

                    // Find csv files from tso + part name
                    // TODO(siyun): divide files with different schema_ts into different splits
                    Pair<List<String>, List<String>> files =
                        columnarManager.findFileNames(tso, logicalSchema, logicalTable, partName);
                    List<String> orcFiles = files.getKey();
                    List<String> csvFiles = files.getValue();

                    allOrcFiles.addAll(orcFiles);

                    if (GeneralUtil.isNotEmpty(csvFiles)) {
                        allCsvFiles.put(
                            physicalTable,
                            csvFiles
                        );
                    }
                }
            }

            if (executionContext.isReadCsvOnly()) {
                // Special hint, only read csv files, so we clear orc files here.
                // Normal columnar read should not get here.
                allOrcFiles.clear();
            }

            if (allOrcFiles.isEmpty() && allCsvFiles.isEmpty()) {
                return ImmutableList.of();
            }

            // correct project column indexes (skip implicit column)
            ImmutableList<Integer> projectList = ossTableScan.getOrcNode().getInProjects();

            // TODO(siyun): column mapping may defer for different files
            List<Integer> correctedProjectList =
                columnarManager.getPhysicalColumnIndexes(tso, null, projectList);

            deltaReadOption.setAllCsvFiles(allCsvFiles);
            deltaReadOption.setProjectColumnIndexes(correctedProjectList);
            return ImmutableList.of(new OssSplit(logicalSchema, physicalSchema, params,
                logicalTable, phyTableList, allOrcFiles, deltaReadOption,
                tso, NO_PARTITION_INFO, false));
        } else {
            List<OssSplit> splits = new ArrayList<>();
            // for each physical table
            for (String phyTable : phyTableList) {
                List<FileMeta> fileMetas = flatFileMetas.get(phyTable);
                if (fileMetas.isEmpty()) {
                    continue;
                }
                //map version to files
                Map<Long, List<String>> fileNamesMap = new HashMap<>();
                for (FileMeta fileMeta : fileMetas) {
                    List<String> list =
                        fileNamesMap.computeIfAbsent(fileMeta.getCommitTs(), aLong -> new ArrayList<>());
                    list.add(fileMeta.getFileName());
                }

                // build single physical table list and params,
                // and split for each file group.
                List<String> singlePhyTableNameList = ImmutableList.of(phyTable);

                Map<Integer, ParameterContext> params =
                    phyOperationBuilder.buildSplitParamMap(singlePhyTableNameList);

                for (List<String> names : fileNamesMap.values()) {
                    OssSplit ossSplit = new OssSplit(logicalSchema, physicalSchema, params,
                        logicalTable, singlePhyTableNameList, names, null,
                        null, NO_PARTITION_INFO, false);
                    splits.add(ossSplit);
                }
            }
            return splits;
        }
    }

    public static List<OssSplit> getFileConcurrencySplit(OSSTableScan ossTableScan, RelNode relNode,
                                                         ExecutionContext executionContext, Long tso) {
        Preconditions.checkArgument(relNode instanceof PhyTableOperation);
        List<OssSplit> splits = new ArrayList<>();

        PhyTableOperation phyTableOperation = (PhyTableOperation) relNode;
        String logicalSchema = phyTableOperation.getSchemaName();
        String physicalSchema = phyTableOperation.getDbIndex();

        String logicalTableName = phyTableOperation.getLogicalTableNames().get(0);
        List<String> phyTableNameList = phyTableOperation.getTableNames().get(0);

        PhyTableScanBuilder phyOperationBuilder =
            (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();

        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTableName);

        if (ossTableScan.isColumnarIndex()) {
            final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            final ColumnarManager columnarManager = ColumnarManager.getInstance();

            // for each physical table
            Set<String> targetOrcFiles = null;
            for (int i = 0; i < phyTableNameList.size(); i++) {
                String phyTable = phyTableNameList.get(i);

                // Find part name from physical schema + physical table
                final String partName = partitionInfo.getPartitionNameByPhyLocation(physicalSchema, phyTable);

                // Find csv files from tso + part name
                Pair<List<String>, List<String>> files =
                    columnarManager.findFileNames(tso, logicalSchema, logicalTableName, partName);
                List<String> orcFiles = files.getKey();
                List<String> csvFiles = files.getValue();

                if (executionContext.isReadOrcOnly()) {
                    // Special hint, only read specified files.
                    // Normal columnar read should not get here.
                    csvFiles = new ArrayList<>();
                    if (null == targetOrcFiles) {
                        targetOrcFiles = new HashSet<>(executionContext.getReadOrcFiles());
                    }
                    List<String> newOrcFiles = new ArrayList<>();
                    for (String orcFile : orcFiles) {
                        if (targetOrcFiles.contains(orcFile)) {
                            newOrcFiles.add(orcFile);
                        }
                    }
                    orcFiles = newOrcFiles;
                }

                if (executionContext.isReadCsvOnly()) {
                    // Special hint, only read csv files, so we clear orc files here.
                    orcFiles.clear();
                }

                // get partition number of this split, used for partition wise join
                RelPartitionWise partitionWise = ossTableScan.getTraitSet().getPartitionWise();
                boolean needPartition = partitionWise.isRemotePartition() || executionContext.getParamManager()
                    .getBoolean(ConnectionParams.SCHEDULE_BY_PARTITION);

                int partition = needPartition ?
                    calcPartition(logicalSchema, logicalTableName, physicalSchema, phyTable) : NO_PARTITION_INFO;
                boolean localPairWise =
                    executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                        && partitionWise.isLocalPartition();

                // build split for orc files.
                if (!orcFiles.isEmpty()) {
                    // build single physical table list and params,
                    // and split for each file.
                    List<String> singlePhyTableNameList = ImmutableList.of(phyTable);

                    Map<Integer, ParameterContext> params =
                        phyOperationBuilder.buildSplitParamMap(singlePhyTableNameList);

                    for (String orcFile : orcFiles) {
                        OssSplit ossSplit = new OssSplit(
                            logicalSchema, physicalSchema, params,
                            logicalTableName, singlePhyTableNameList,
                            ImmutableList.of(orcFile), null,
                            tso, partition, localPairWise);
                        splits.add(ossSplit);
                    }
                }

                // build split for csv files.
                if (!csvFiles.isEmpty()) {
                    for (String csvFile : csvFiles) {
                        // build single physical table list and params,
                        // and split for each file.
                        List<String> singlePhyTableNameList = ImmutableList.of(phyTable);

                        Map<Integer, ParameterContext> params =
                            phyOperationBuilder.buildSplitParamMap(singlePhyTableNameList);

                        // Build delta read option. There is only one physical table and one csv file name.
                        DeltaReadOption deltaReadOption = new DeltaReadOption(tso);
                        Map<String, List<String>> allCsvFiles = new HashMap<>();
                        allCsvFiles.put(phyTable, ImmutableList.of(csvFile));

                        // correct project column indexes (skip implicit column)
                        ImmutableList<Integer> projectList = ossTableScan.getOrcNode().getInProjects();

                        // TODO(siyun): column mapping may defer for different files
                        List<Integer> correctedProjectList =
                            columnarManager.getPhysicalColumnIndexes(tso, null, projectList);

                        deltaReadOption.setAllCsvFiles(allCsvFiles);
                        deltaReadOption.setProjectColumnIndexes(correctedProjectList);

                        OssSplit ossSplit = new OssSplit(
                            logicalSchema, physicalSchema, params,
                            logicalTableName, singlePhyTableNameList,
                            null, deltaReadOption,
                            tso, partition, localPairWise);
                        splits.add(ossSplit);
                    }
                }
            }
            return splits;
        } else {
            Map<String, List<FileMeta>> flatFileMetas = tableMeta.getFlatFileMetas();

            // for each physical table
            for (int i = 0; i < phyTableNameList.size(); i++) {
                String phyTable = phyTableNameList.get(i);
                List<FileMeta> fileMetas = flatFileMetas.get(phyTable);
                if (fileMetas.isEmpty()) {
                    continue;
                }

                // build single physical table list and params,
                // and split for each file.
                List<String> singlePhyTableNameList = ImmutableList.of(phyTable);

                Map<Integer, ParameterContext> params =
                    phyOperationBuilder.buildSplitParamMap(singlePhyTableNameList);

                for (FileMeta fileMeta : fileMetas) {
                    OssSplit ossSplit = new OssSplit(logicalSchema, physicalSchema, params,
                        logicalTableName, singlePhyTableNameList, ImmutableList.of(fileMeta.getFileName()), null,
                        null, NO_PARTITION_INFO, false);
                    splits.add(ossSplit);
                }
            }
            return splits;
        }
    }

    public static int calcPartition(String logicalSchema, String logicalTableName, String physicalSchema,
                                    String physicalTableName) {
        PartitionInfo partInfo =
            OptimizerContext.getContext(logicalSchema).getPartitionInfoManager()
                .getPartitionInfo(logicalTableName);
        int partition = partInfo.getPartitionBy().getPartitions().stream().
            filter(
                t -> t.getLocation().getGroupKey().equalsIgnoreCase(physicalSchema)
                    && t.getLocation().getPhyTableName().equalsIgnoreCase(physicalTableName))
            .findFirst().map(PartSpecBase::getPosition).map(Long::intValue).orElse(-1);
        return partition - 1;
    }

    @JsonIgnore
    public List<OSSReadOption> getReadOptions() {
        return readOptions;
    }

    @JsonProperty
    public String getLogicalTableName() {
        return logicalTableName;
    }

    @JsonProperty
    public List<String> getPhyTableNameList() {
        return phyTableNameList;
    }

    @JsonProperty
    public List<String> getDesignatedFile() {
        return designatedFile;
    }

    @JsonIgnore
    public List<List<FileMeta>> getAllFileMetas() {
        return allFileMetas;
    }

    @JsonProperty
    public String getLogicalSchema() {
        return logicalSchema;
    }

    @JsonProperty
    public String getPhysicalSchema() {
        return physicalSchema;
    }

    @JsonIgnore
    public Map<Integer, ParameterContext> getParams() {
        if (params == null && paramsBytes != null) {
            params = (Map<Integer, ParameterContext>) SerializeUtils.deFromBytes(paramsBytes, Map.class);
        }
        return params;
    }

    @JsonProperty
    public byte[] getParamsBytes() {
        if (paramsBytes == null && params != null) {
            paramsBytes = SerializeUtils.getBytes((Serializable) params);
        }
        return paramsBytes;
    }

    @JsonProperty
    public int getNodePartCount() {
        return nodePartCount;
    }

    public void setNodePartCount(int nodePartCount) {
        this.nodePartCount = nodePartCount;
    }

    @JsonProperty
    public Boolean isLocalPairWise() {
        return localPairWise;
    }

    public void setLocalPairWise(Boolean localPairWise) {
        this.localPairWise = localPairWise;
    }

    @JsonIgnore
    public TypeDescription getReadSchema() {
        return readSchema;
    }

    @JsonIgnore
    public SearchArgument getSearchArgument() {
        return searchArgument;
    }

    @JsonIgnore
    public String[] getColumns() {
        return columns;
    }

    public OSSColumnTransformer getColumnTransformer(OSSTableScan ossTableScan, ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTableName);
        Set<String> filterSet = getFilterSet(executionContext);
        List<Timestamp> timestamps = new ArrayList<>();
        List<ColumnMeta> columnMetas = new ArrayList<>();
        List<ColumnMeta> fileColumnMetas = new ArrayList<>();
        List<ColumnMeta> initColumnMetas = new ArrayList<>();
        List<Integer> locInOrc = new ArrayList<>();

        boolean isColumnarMode = ossTableScan.isColumnarIndex();

        if (isColumnarMode) {
            String desinatedFileName;
            if (CollectionUtils.isEmpty(designatedFile)) {
                // for csv file
                Optional<List<String>> fileNames = deltaReadOption.allCsvFiles.values().stream().findFirst();
                if (fileNames.isPresent()) {
                    if (fileNames.get().isEmpty()) {
                        return null;
                    } else {
                        desinatedFileName = fileNames.get().get(0);
                    }
                } else {
                    return null;
                }
            } else {
                // for orc file
                desinatedFileName = designatedFile.get(0);
            }
            FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(desinatedFileName);
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
        } else {
            Map<String, List<FileMeta>> flatFileMetas = tableMeta.getFlatFileMetas();
            Optional<FileMeta> baseFileMeta = phyTableNameList.stream()
                .map(flatFileMetas::get)
                .flatMap(List::stream)
                .filter(x -> (filterSet == null || filterSet.contains(x.getFileName())))
                .findFirst();

            if (!baseFileMeta.isPresent()) {
                return null;
            }

            OSSOrcFileMeta fileMeta = (OSSOrcFileMeta) baseFileMeta.get();

            Preconditions.checkArgument(fileMeta.getCommitTs() != null);
            for (String column : ossTableScan.getOrcNode().getInputProjectName()) {
                String fieldId = tableMeta.getColumnFieldId(column);
                columnMetas.add(tableMeta.getColumn(column));
                if (tableMeta.isOldFileStorage()) {
                    fileColumnMetas.add(tableMeta.getColumn(fieldId));
                    initColumnMetas.add(null);
                    timestamps.add(null);
                    locInOrc.add(fileMeta.getColumnNameToIdx(fieldId) + 1);
                    continue;
                }
                ColumnMetaWithTs meta = OrcColumnManager.getHistoryWithTs(fieldId, fileMeta.getCommitTs());
                if (meta != null) {
                    fileColumnMetas.add(meta.getMeta());
                    initColumnMetas.add(null);
                    timestamps.add(meta.getCreate());
                    locInOrc.add(fileMeta.getColumnNameToIdx(fieldId) + 1);
                } else {
                    // new column after the file was created, use default value when the column was created
                    fileColumnMetas.add(null);
                    ColumnMetaWithTs versionColumnMeta = OrcColumnManager.getFirst(fieldId);
                    initColumnMetas.add(versionColumnMeta.getMeta());
                    timestamps.add(versionColumnMeta.getCreate());
                    locInOrc.add(null);
                }
            }
        }

        return new OSSColumnTransformer(
            columnMetas,
            fileColumnMetas,
            initColumnMetas,
            timestamps,
            locInOrc
        );
    }

    public void init(OSSTableScan ossTableScan, ExecutionContext executionContext,
                     SessionProperties sessionProperties, Map<Integer, BloomFilterInfo> bloomFilterInfos,
                     RexNode bloomFilterCondition) {
        if (isInit) {
            return;
        }
        Preconditions.checkArgument(this.readSchema == null);
        Preconditions.checkArgument(this.allFileMetas == null);
        Preconditions.checkArgument(this.searchArgument == null);
        Preconditions.checkArgument(this.columns == null);
        Preconditions.checkArgument(this.readOptions == null);

        Set<String> filterSet = getFilterSet(executionContext);

        String pattern = executionContext.getParamManager().getString(ConnectionParams.FILE_PATTERN);
        Pattern complied = TStringUtil.isEmpty(pattern) ? null : Pattern.compile(pattern);

        List<List<FileMeta>> allFileMetas = new ArrayList<>();

        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTableName);
        Parameters parameters = executionContext.getParams();
        Engine tableEngine = tableMeta.getEngine();

        List<ColumnMeta> columnMetas = new ArrayList<>();
        List<ColumnMeta> fileColumnMetas = new ArrayList<>();
        List<ColumnMeta> initColumnMetas = new ArrayList<>();
        List<Timestamp> timestamps = new ArrayList<>();

        boolean isColumnarMode = ossTableScan.isColumnarIndex();
        // init allFileMetas and readSchema
        for (int j = 0; j < phyTableNameList.size(); j++) {
            String phyTable = phyTableNameList.get(j);
            List<FileMeta> fileMetas;
            if (isColumnarMode) {
                final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
                final ColumnarManager columnarManager = ColumnarManager.getInstance();
                final long checkpointTso = this.checkpointTso;

                // Find part name from physical schema + physical table
                final String partName = partitionInfo.getPartitionNameByPhyLocation(physicalSchema, phyTable);

                // Find csv files from tso + part name
                Pair<List<FileMeta>, List<FileMeta>> files =
                    columnarManager.findFiles(checkpointTso, logicalSchema, logicalTableName, partName);
                fileMetas = files.getKey();
            } else {
                // physical table name -> file metas
                Map<String, List<FileMeta>> flatFileMetas = tableMeta.getFlatFileMetas();
                fileMetas = flatFileMetas.get(phyTable).stream()
                    .filter(x -> (filterSet == null || filterSet.contains(x.getFileName()))).collect(
                        Collectors.toList());
            }

            if (fileMetas.isEmpty()) {
                continue;
            }
            allFileMetas.add(fileMetas);

            if (this.readSchema == null) {
                TypeDescription typeDescription = TypeDescription.createStruct();
                OSSOrcFileMeta fileMeta = (OSSOrcFileMeta) fileMetas.get(0);

                if (isColumnarMode) {

                    // for columnar store mode, add implicit column: position
                    // NOTE: need destruction in read result post handler.
                    // TODO(siyun): this part is only used by old table scan, support later
                    typeDescription.addField("position", TypeDescription.createLong());
                    for (Integer columnIndex : ossTableScan.getOrcNode().getInProjects()) {
                        int actualColumnIndex = columnIndex + (ColumnarStoreUtils.POSITION_COLUMN_INDEX + 1);
                        typeDescription.addField(
                            fileMeta.getTypeDescription().getFieldNames().get(actualColumnIndex),
                            fileMeta.getTypeDescription().getChildren().get(actualColumnIndex).clone());
                        columnMetas.add(fileMeta.getColumnMetas().get(actualColumnIndex));
                        fileColumnMetas.add(fileMeta.getColumnMetas().get(actualColumnIndex));
                        initColumnMetas.add(null);
                    }
                } else {
                    if (fileMeta.getCommitTs() == null) {
                        continue;
                    }
                    for (String column : ossTableScan.getOrcNode().getInputProjectName()) {
                        String fieldId = tableMeta.getColumnFieldId(column);
                        columnMetas.add(tableMeta.getColumn(column));
                        if (tableMeta.isOldFileStorage()) {
                            Integer columnIndex = fileMeta.getColumnNameToIdx(fieldId);
                            typeDescription.addField(
                                fileMeta.getTypeDescription().getFieldNames().get(columnIndex),
                                fileMeta.getTypeDescription().getChildren().get(columnIndex).clone());
                            fileColumnMetas.add(tableMeta.getColumn(fieldId));
                            initColumnMetas.add(null);
                            timestamps.add(null);
                            continue;
                        }
                        ColumnMetaWithTs meta = OrcColumnManager.getHistoryWithTs(fieldId, fileMeta.getCommitTs());
                        if (meta != null) {
                            Integer columnIndex = fileMeta.getColumnNameToIdx(fieldId);
                            typeDescription.addField(
                                fileMeta.getTypeDescription().getFieldNames().get(columnIndex),
                                fileMeta.getTypeDescription().getChildren().get(columnIndex).clone());
                            fileColumnMetas.add(meta.getMeta());
                            initColumnMetas.add(null);
                            timestamps.add(meta.getCreate());
                        } else {
                            // new column after the file was created, use default value when the column was created
                            fileColumnMetas.add(null);
                            ColumnMetaWithTs versionColumnMeta = OrcColumnManager.getFirst(fieldId);
                            initColumnMetas.add(versionColumnMeta.getMeta());
                            timestamps.add(versionColumnMeta.getCreate());
                        }
                    }
                }

                this.readSchema = typeDescription;
            }
        }
        this.allFileMetas = allFileMetas;

        OSSColumnTransformer ossColumnTransformer = new OSSColumnTransformer(columnMetas,
            fileColumnMetas,
            initColumnMetas,
            timestamps,
            null);
        // init readOptions
        List<OSSReadOption> phyTableReadOptions = new ArrayList<>();

        if (this.readSchema == null) {
            this.readOptions = phyTableReadOptions;
            this.isInit = true;
            return;
        }

        List<RexNode> conditions = new ArrayList<>();
        if (!ossTableScan.getOrcNode().getFilters().isEmpty()) {
            conditions.add(ossTableScan.getOrcNode().getFilters().get(0));
        }
        if (bloomFilterCondition != null) {
            conditions.add(bloomFilterCondition);
        }
        switch (conditions.size()) {
        case 0:
            buildSearchArgumentAndColumns();
            break;
        case 1:
            buildSearchArgumentAndColumns(
                ossTableScan,
                conditions.get(0),
                parameters,
                sessionProperties,
                ossColumnTransformer,
                bloomFilterInfos,
                (OSSOrcFileMeta) allFileMetas.get(0).get(0));
            break;
        case 2:
            RexBuilder rexBuilder = ossTableScan.getCluster().getRexBuilder();
            buildSearchArgumentAndColumns(
                ossTableScan,
                rexBuilder.makeCall(TddlOperatorTable.AND, conditions),
                parameters,
                sessionProperties,
                ossColumnTransformer,
                bloomFilterInfos,
                (OSSOrcFileMeta) allFileMetas.get(0).get(0));
        }

        Long readTs = null;
        if (ossTableScan.getFlashback() instanceof RexDynamicParam) {

            String timestampString = executionContext.getParams().getCurrentParameter()
                .get(((RexDynamicParam) ossTableScan.getFlashback()).getIndex() + 1).getValue().toString();
            TimeZone fromTimeZone;
            if (executionContext.getTimeZone() != null) {
                fromTimeZone = executionContext.getTimeZone().getTimeZone();
            } else {
                fromTimeZone = TimeZone.getDefault();
            }
            readTs = OSSTaskUtils.getTsFromTimestampWithTimeZone(timestampString, fromTimeZone);
        }

        for (int j = 0; j < allFileMetas.size(); j++) {
            String phyTable = phyTableNameList.get(j);
            List<FileMeta> phyTableFileMetas = allFileMetas.get(j);
            List<FileMeta> afterPruningFileMetas = new ArrayList<>();
            List<PruningResult> pruningResultList = new ArrayList<>();

            // filter pruning
            for (FileMeta fileMeta : phyTableFileMetas) {
                OssOrcFilePruner ossOrcFilePruner = new OssOrcFilePruner((OSSOrcFileMeta) fileMeta, searchArgument,
                    filterSet, complied, readTs, tableMeta);
                PruningResult pruningResult = ossOrcFilePruner.prune();
                if (pruningResult.skip()) {
                    continue;
                }
                afterPruningFileMetas.add(fileMeta);
                if (!ossTableScan.withAgg()) {
                    pruningResultList.add(pruningResult);
                    continue;
                }

                // with agg, choose statistics or orc file for each stripe
                AggPruningResult aggPruningResult = ((OrcFilePruningResult) pruningResult).toStatistics();
                //get column stripe
                String primaryCol = tableMeta.getPrimaryIndex().getKeyColumns().get(0).getName();
                String fieldId = tableMeta.getColumnFieldId(primaryCol);

                Map<Long, StripeColumnMeta> stripeMap = ((OSSOrcFileMeta) fileMeta).getStripeColumnMetas(fieldId);

                // filter on missing column or converted column, don't use statistics
                if (!enableAggPruner || stripeMap == null) {
                    pruningResultList.add(aggPruningResult);
                    continue;
                }
                if (pruneAgg(ossTableScan, (OSSOrcFileMeta) fileMeta, ossColumnTransformer)) {
                    // a pass should be transformed to a part with all stripes
                    if (aggPruningResult.pass()) {
                        aggPruningResult = new AggPruningResult(stripeMap);
                    }
                } else {
                    aggPruningResult = AggPruningResult.NO_SCAN;
                }
                // prune all the stripe
                if (aggPruningResult.part()) {
                    OssAggPruner ossAggPruner =
                        new OssAggPruner((OSSOrcFileMeta) fileMeta, searchArgument, aggPruningResult,
                            tableMeta);
                    ossAggPruner.prune();
                    pruneStripe((OSSOrcFileMeta) fileMeta, ossTableScan, aggPruningResult,
                        ossColumnTransformer,
                        tableMeta);
                    // all stripes can use statistics, use file statistics instead
                    if (aggPruningResult.getStripeMap().size() == stripeMap.size()) {
                        if (aggPruningResult.getNonStatisticsStripeSize() == 0) {
                            aggPruningResult = AggPruningResult.NO_SCAN;
                        }
                        if (aggPruningResult.getNonStatisticsStripeSize() == stripeMap.size()) {
                            aggPruningResult = AggPruningResult.PASS;
                        }
                    }
                    aggPruningResult.log();
                }
                pruningResultList.add(aggPruningResult);
            }

            if (afterPruningFileMetas.isEmpty()) {
                continue;
            }

            List<String> tableFileNames = afterPruningFileMetas
                .stream()
                .map(FileMeta::getFileName)
                .collect(Collectors.toList());

            OSSReadOption readOption = new OSSReadOption(
                getReadSchema(),
                ossColumnTransformer,
                searchArgument,
                columns,
                phyTable,
                tableEngine,
                tableFileNames,
                afterPruningFileMetas,
                pruningResultList,
                executionContext.getParamManager().getLong(ConnectionParams.OSS_ORC_MAX_MERGE_DISTANCE),
                ossTableScan.isColumnarIndex()
            );

            phyTableReadOptions.add(readOption);
        }
        this.readOptions = phyTableReadOptions;
        this.isInit = true;
    }

    /**
     * check whether to prune agg in stripe-level using statistics.
     * Currently, don't support agg for column Type change.
     * In this case, return true directly.
     *
     * @param ossTableScan the table scan which the agg belongs to
     * @param fileMeta the meta of current file
     * @return true if we should try to prune each stripe
     */
    private boolean pruneAgg(OSSTableScan ossTableScan,
                             OSSOrcFileMeta fileMeta,
                             OSSColumnTransformer ossColumnTransformer) {
        // with filter, should prune stripes
        if (!ossTableScan.getOrcNode().getFilters().isEmpty()) {
            return true;
        }
        // count null values, should prune stripes
        LogicalAggregate agg = ossTableScan.getAgg();
        for (int i = 0; i < ossTableScan.getAggColumns().size(); i++) {
            SqlKind kind = agg.getAggCallList().get(i).getAggregation().getKind();
            RelColumnOrigin columnOrigin = ossTableScan.getAggColumns().get(i);
            // no column specified
            if (columnOrigin == null) {
                continue;
            }
            TypeComparison ossColumnCompare = ossColumnTransformer.compare(columnOrigin.getColumnName());

            if (kind == SqlKind.COUNT) {
                // can't deal with type conversion
                if (ossColumnCompare == TypeComparison.IS_EQUAL_NO) {
                    return true;
                }

                // missing column, using 0 is null or rowCount if not null
                if (TypeComparison.isMissing(ossColumnCompare)) {
                    continue;
                }

                if (CBOUtil.getTableMeta(columnOrigin.getOriginTable()).
                    getColumn(columnOrigin.getColumnName()).isNullable()) {
                    return true;
                }
            }
            if (kind == SqlKind.SUM || kind == SqlKind.SUM0) {
                // can't deal with type conversion
                if (ossColumnCompare == TypeComparison.IS_EQUAL_NO) {
                    return true;
                }

                // can't deal with missing column
                if (TypeComparison.isMissing(ossColumnCompare)) {
                    return true;
                }

                ColumnStatistics columnStatistics = fileMeta.getStatisticsMap().get(columnOrigin.getColumnName());
                if (columnStatistics instanceof IntegerColumnStatistics) {
                    // sum overflow, can't use statistics
                    if (!((IntegerColumnStatistics) columnStatistics).isSumDefined()) {
                        return true;
                    }
                }
            }

            if (kind == SqlKind.MIN || kind == SqlKind.MAX) {
                // can't deal with type conversion
                if (ossColumnCompare == TypeComparison.IS_EQUAL_NO) {
                    return true;
                }
            }
        }
        return false;
    }

    private void pruneStripe(OSSOrcFileMeta fileMeta,
                             OSSTableScan ossTableScan,
                             AggPruningResult pruningResult,
                             OSSColumnTransformer ossColumnTransformer,
                             TableMeta tableMeta) {
        LogicalAggregate agg = ossTableScan.getAgg();
        for (int i = 0; i < ossTableScan.getAggColumns().size(); i++) {
            SqlKind kind = agg.getAggCallList().get(i).getAggregation().getKind();
            RelColumnOrigin columnOrigin = ossTableScan.getAggColumns().get(i);
            // any stripe can't use statistics
            if (kind == SqlKind.COUNT || kind == SqlKind.CHECK_SUM || kind == SqlKind.CHECK_SUM_V2) {
                if (!(pruningResult.getNonStatisticsStripeSize() == 0)) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                }
            }
            if (columnOrigin == null) {
                continue;
            }

            TypeComparison ossColumnCompare = ossColumnTransformer.compare(columnOrigin.getColumnName());

            if (kind == SqlKind.COUNT) {
                // can't deal with type conversion
                if (ossColumnCompare == TypeComparison.IS_EQUAL_NO) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                    continue;
                }

                // can deal with missing column
                if (TypeComparison.isMissing(ossColumnCompare)) {
                    continue;
                }
                // for count, null can't use statistics
                String fieldId = tableMeta.getColumnFieldId(columnOrigin.getColumnName());
                if (fileMeta.getStatisticsMap().get(fieldId).hasNull()) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                }
            }
            // for sum, overflow can't use statistics
            if (kind == SqlKind.SUM || kind == SqlKind.SUM0) {
                // can't deal with type conversion
                if (ossColumnCompare == TypeComparison.IS_EQUAL_NO) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                    continue;
                }

                // can't deal with missing column
                if (TypeComparison.isMissing(ossColumnCompare)) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                    continue;
                }

                String fieldId = tableMeta.getColumnFieldId(columnOrigin.getColumnName());
                Map<Long, StripeColumnMeta> columnMetaMap = fileMeta.getStripeColumnMetas(fieldId);
                for (Long index : pruningResult.getStripeMap().keySet()) {
                    ColumnStatistics columnStatistics = columnMetaMap.get(index).getColumnStatistics();
                    if (columnStatistics instanceof IntegerColumnStatistics) {
                        if (!((IntegerColumnStatistics) columnStatistics).isSumDefined()) {
                            pruningResult.addNotAgg(index);
                        }
                    }
                }
            }
            if (kind == SqlKind.MIN || kind == SqlKind.MAX) {
                // can't deal with type conversion
                if (ossColumnCompare == TypeComparison.IS_EQUAL_NO) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                }
            }
        }
    }

    @Nullable
    public Set<String> getFilterSet(ExecutionContext executionContext) {
        Set<String> filterSet = null;
        String fileListStr = executionContext.getParamManager().getString(ConnectionParams.FILE_LIST);
        if (!fileListStr.equalsIgnoreCase("ALL")) {
            filterSet = Arrays.stream(fileListStr.split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
        }

        if (this.designatedFile != null) {
            if (filterSet != null) {
                filterSet = ImmutableSet.<String>builder().addAll(filterSet).addAll(designatedFile).build();
            } else {
                filterSet = ImmutableSet.copyOf(designatedFile);
            }
        }

        return filterSet;
    }

    private void buildSearchArgumentAndColumns() {
        searchArgument = SearchArgumentFactory
            .newBuilder()
            .literal(SearchArgument.TruthValue.YES_NO)
            .build();
        columns = null;
        this.enableAggPruner = true;
    }

    private void buildSearchArgumentAndColumns(OSSTableScan ossTableScan,
                                               RexNode rexNode,
                                               Parameters parameters,
                                               SessionProperties sessionProperties,
                                               OSSColumnTransformer ossColumnTransformer,
                                               Map<Integer, BloomFilterInfo> bloomFilterInfos,
                                               OSSOrcFileMeta fileMeta) {
        // init searchArgument and columns
        OSSPredicateBuilder predicateBuilder =
            new OSSPredicateBuilder(parameters,
                ossTableScan.getOrcNode().getInputProjectRowType().getFieldList(),
                bloomFilterInfos, ossTableScan.getOrcNode().getRowType().getFieldList(),
                CBOUtil.getTableMeta(ossTableScan.getTable()), sessionProperties,
                ossColumnTransformer, fileMeta);
        Boolean valid = rexNode.accept(predicateBuilder);
        this.enableAggPruner = predicateBuilder.isEnableAggPruner();
        if (valid != null && valid.booleanValue()) {
            searchArgument = predicateBuilder.build();
            columns = predicateBuilder.columns();
        } else {
            // full scan
            searchArgument = SearchArgumentFactory
                .newBuilder()
                .literal(SearchArgument.TruthValue.YES_NO)
                .build();
            columns = null;
        }
    }

    @JsonIgnore
    public boolean isInit() {
        return isInit;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("logicalSchema", logicalSchema)
            .add("physicalSchema", physicalSchema)
            .add("logicalTableName", logicalTableName)
            .add("phyTableNameList", phyTableNameList)
            .add("params", params)
            .toString();
    }

    public static class DeltaReadOption implements Serializable {
        /**
         * checkpoint tso for columnar store.
         */
        private final long checkpointTso;

        /**
         * map: {physical table} -> {list <csv>}
         */
        private Map<String, List<String>> allCsvFiles;

        private List<Integer> projectColumnIndexes;

        public DeltaReadOption(long checkpointTso) {
            this.checkpointTso = checkpointTso;
        }

        @JsonCreator
        public DeltaReadOption(
            @JsonProperty("checkpointTso") long checkpointTso,
            @JsonProperty("allCsvFiles") Map<String, List<String>> allCsvFiles,
            @JsonProperty("projectColumnIndexes") List<Integer> projectColumnIndexes) {
            this.checkpointTso = checkpointTso;
            this.allCsvFiles = allCsvFiles;
            this.projectColumnIndexes = projectColumnIndexes;
        }

        @JsonProperty
        public long getCheckpointTso() {
            return checkpointTso;
        }

        @JsonProperty
        public Map<String, List<String>> getAllCsvFiles() {
            return allCsvFiles;
        }

        @JsonProperty
        public List<Integer> getProjectColumnIndexes() {
            return projectColumnIndexes;
        }

        public void setAllCsvFiles(Map<String, List<String>> allCsvFiles) {
            this.allCsvFiles = allCsvFiles;
        }

        public void setProjectColumnIndexes(List<Integer> projectColumnIndexes) {
            this.projectColumnIndexes = projectColumnIndexes;
        }
    }
}
