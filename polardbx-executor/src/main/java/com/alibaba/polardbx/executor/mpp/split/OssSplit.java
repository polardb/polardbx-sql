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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.SerializeUtils;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.archive.reader.OSSReadOption;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.SearchArgument;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.utils.ITimestampOracle.BITS_LOGICAL_TIME;
import static com.google.common.base.MoreObjects.toStringHelper;

public class OssSplit implements ConnectorSplit {
    private List<OSSReadOption> readOptions;

    private String logicalSchema;
    private String physicalSchema;

    // all physical tables in a logical table share the parameters
    private Map<Integer, ParameterContext> params;

    private TypeDescription readSchema;

    // search argument / columns
    // all physical tables in a logical table share the search arguments / columns.
    private SearchArgument searchArgument;
    private String[] columns;

    private String logicalTableName;
    private List<String> phyTableNameList;
    private List<List<FileMeta>> allFileMetas;
    private String designatedFile;
    private byte[] paramsBytes;

    private boolean isInit = false;

    public OssSplit(String logicalSchema, String physicalSchema,
                    Map<Integer, ParameterContext> params,
                    String logicalTableName,
                    List<String> phyTableNameList,
                    String designatedFile) {
        this.logicalSchema = logicalSchema;
        this.physicalSchema = physicalSchema;
        this.params = params;
        this.logicalTableName = logicalTableName;
        this.phyTableNameList = phyTableNameList;
        this.designatedFile = designatedFile;
    }

    @JsonCreator
    public OssSplit(
        @JsonProperty("logicalSchema") String logicalSchema,
        @JsonProperty("physicalSchema") String physicalSchema,
        @JsonProperty("paramsBytes") byte[] paramsBytes,
        @JsonProperty("logicalTableName") String logicalTableName,
        @JsonProperty("phyTableNameList") List<String> phyTableNameList,
        @JsonProperty("designatedFile") String designatedFile) {
        this.logicalSchema = logicalSchema;
        this.physicalSchema = physicalSchema;
        this.paramsBytes = paramsBytes;
        this.logicalTableName = logicalTableName;
        this.phyTableNameList = phyTableNameList;
        this.designatedFile = designatedFile;
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

    public static OssSplit getTableConcurrencySplit(OSSTableScan ossTableScan, RelNode relNode,
                                                    ExecutionContext executionContext) {
        Preconditions.checkArgument(relNode instanceof PhyTableOperation);
        PhyTableOperation phyTableOperation = (PhyTableOperation) relNode;
        String logicalSchema = phyTableOperation.getSchemaName();
        String physicalSchema = phyTableOperation.getDbIndex();

        PhyTableScanBuilder phyOperationBuilder =
            (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();

        Map<Integer, ParameterContext> params =
            phyOperationBuilder.buildSplitParamMap(phyTableOperation.getTableNames().get(0));
        return new OssSplit(
            logicalSchema, physicalSchema, params,
            phyTableOperation.getLogicalTableNames().get(0), phyTableOperation.getTableNames().get(0), null);
    }

    public static List<OssSplit> getFileConcurrencySplit(OSSTableScan ossTableScan, RelNode relNode,
                                                         ExecutionContext executionContext) {
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
                    logicalTableName, singlePhyTableNameList, fileMeta.getFileName());
                splits.add(ossSplit);
            }
        }

        return splits;
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
    public String getDesignatedFile() {
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

    public void init(OSSTableScan ossTableScan, ExecutionContext executionContext, SearchArgument searchArgument,
                     String[] columns) {
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
        // physical table name -> file metas
        Map<String, List<FileMeta>> flatFileMetas = tableMeta.getFlatFileMetas();
        Engine tableEngine = tableMeta.getEngine();

        List<ColumnMeta> columnMetas = new ArrayList<>();

        // init allFileMetas and readSchema
        for (int j = 0; j < phyTableNameList.size(); j++) {
            String phyTable = phyTableNameList.get(j);
            List<FileMeta> fileMetas = flatFileMetas.get(phyTable);
            if (fileMetas.isEmpty()) {
                continue;
            }
            allFileMetas.add(fileMetas);

            if (this.readSchema == null) {
                OSSOrcFileMeta fileMeta = (OSSOrcFileMeta) fileMetas.get(0);
                TypeDescription typeDescription = TypeDescription.createStruct();
                for (Integer columnIndex : ossTableScan.getOrcNode().getInProjects()) {
                    typeDescription.addField(
                        fileMeta.getTypeDescription().getFieldNames().get(columnIndex),
                        fileMeta.getTypeDescription().getChildren().get(columnIndex).clone());
                    columnMetas.add(fileMeta.getColumnMetas().get(columnIndex));
                }
                this.readSchema = typeDescription;
            }
        }
        this.allFileMetas = allFileMetas;

        // init readOptions
        List<OSSReadOption> phyTableReadOptions = new ArrayList<>();

        this.searchArgument = searchArgument;
        this.columns = columns;

        Long readTs = null;
        if (ossTableScan.getFlashback() instanceof RexDynamicParam) {
            Timestamp timestamp = Timestamp.valueOf(executionContext.getParams().getCurrentParameter().get(((RexDynamicParam) ossTableScan.getFlashback()).getIndex() +1).getValue().toString());
            readTs = timestamp.getTime() << BITS_LOGICAL_TIME;
        }

        for (int j = 0; j < allFileMetas.size(); j++) {
            String phyTable = phyTableNameList.get(j);
            List<FileMeta> phyTableFileMetas = allFileMetas.get(j);
            List<FileMeta> afterPruningFileMetas = new ArrayList<>();
            List<PruningResult> pruningResultList = new ArrayList<>();

            // bloomFilter pruning
            for (FileMeta fileMeta : phyTableFileMetas) {
                OssOrcFilePruner ossOrcFilePruner = new OssOrcFilePruner((OSSOrcFileMeta) fileMeta, searchArgument,
                    filterSet, complied, readTs);
                PruningResult pruningResult = ossOrcFilePruner.prune();
                if (pruningResult.skip()) {
                    continue;
                }
                afterPruningFileMetas.add(fileMeta);

                // with agg, choose statistics or orc file for each stripe
                if (ossTableScan.withAgg()) {
                    if (pruneAgg(ossTableScan, (OSSOrcFileMeta) fileMeta)) {
                        // a pass should be transformed to a part with all stripes
                        if (pruningResult.pass()) {
                            pruningResult = new PruningResult(
                                ((OSSOrcFileMeta) fileMeta)
                                    .getStripeColumnMetas(fileMeta.getColumnMetas().get(0).getName()));
                        }
                    }
                    // prune all the stripe
                    if (pruningResult.part()) {
                        OssAggPruner ossAggPruner =
                            new OssAggPruner((OSSOrcFileMeta) fileMeta, searchArgument, pruningResult);
                        ossAggPruner.prune();
                        pruneStripe((OSSOrcFileMeta) fileMeta, ossTableScan, pruningResult);
                        // all stripes can use statistics, use file statistics instead
                        if (pruningResult.fullAgg()) {
                            if (pruningResult.getStripeMap().size() == ((OSSOrcFileMeta) fileMeta)
                                .getStripeColumnMetas(fileMeta.getColumnMetas().get(0).getName()).size()) {
                                pruningResult = PruningResult.PASS;
                            }
                        }
                        pruningResult.log();
                    }
                }

                pruningResultList.add(pruningResult);
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
                columnMetas,
                searchArgument,
                columns,
                phyTable,
                tableEngine,
                tableFileNames,
                afterPruningFileMetas,
                pruningResultList,
                executionContext.getParamManager().getLong(ConnectionParams.OSS_ORC_MAX_MERGE_DISTANCE)
            );

            phyTableReadOptions.add(readOption);
        }
        this.readOptions = phyTableReadOptions;
        this.isInit = true;
    }

    /**
     * check whether to prune agg in stripe-level using statistics
     *
     * @param ossTableScan the table scan which the agg belongs to
     * @param fileMeta the meta of current file
     * @return true if we should try to prune each stripe
     */
    private boolean pruneAgg(OSSTableScan ossTableScan, OSSOrcFileMeta fileMeta) {
        // with filter, should prune stripes
        if (!ossTableScan.getOrcNode().getFilters().isEmpty()) {
            return true;
        }
        // count null values, should prune stripes
        LogicalAggregate agg = ossTableScan.getAgg();
        for (int i = 0; i < ossTableScan.getAggColumns().size(); i++) {
            SqlKind kind = agg.getAggCallList().get(i).getAggregation().getKind();
            if (kind == SqlKind.COUNT) {
                RelColumnOrigin columnOrigin = ossTableScan.getAggColumns().get(i);
                if (columnOrigin == null) {
                    continue;
                }
                if (CBOUtil.getTableMeta(columnOrigin.getOriginTable()).
                    getColumn(columnOrigin.getColumnName()).isNullable()) {
                    return true;
                }
            }
            if (kind == SqlKind.SUM) {
                RelColumnOrigin columnOrigin = ossTableScan.getAggColumns().get(i);
                if (columnOrigin == null) {
                    continue;
                }
                ColumnStatistics columnStatistics = fileMeta.getStatisticsMap().get(columnOrigin.getColumnName());
                if (columnStatistics instanceof IntegerColumnStatistics) {
                    // sum overflow, can't use statistics
                    if (!((IntegerColumnStatistics) columnStatistics).isSumDefined()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void pruneStripe(OSSOrcFileMeta fileMeta, OSSTableScan ossTableScan, PruningResult pruningResult) {
        LogicalAggregate agg = ossTableScan.getAgg();
        for (int i = 0; i < ossTableScan.getAggColumns().size(); i++) {
            SqlKind kind = agg.getAggCallList().get(i).getAggregation().getKind();
            RelColumnOrigin columnOrigin = ossTableScan.getAggColumns().get(i);
            // any stripe can't use statistics
            if (kind == SqlKind.COUNT) {
                if (!pruningResult.fullAgg()) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                }
            }
            if (columnOrigin == null) {
                continue;
            }
            // for count, null can't use statistics
            if (kind == SqlKind.COUNT) {
                if (fileMeta.getStatisticsMap().get(columnOrigin.getColumnName()).hasNull()) {
                    pruningResult.addNotAgg(pruningResult.getStripeMap().keySet());
                }
            }
            // for sum, overflow can't use statistics
            if (kind == SqlKind.SUM) {
                Map<Long, StripeColumnMeta> columnMetaMap = fileMeta.getStripeColumnMetas(columnOrigin.getColumnName());
                for (Long index : pruningResult.getStripeMap().keySet()) {
                    ColumnStatistics columnStatistics = columnMetaMap.get(index).getColumnStatistics();
                    if (columnStatistics instanceof IntegerColumnStatistics) {
                        if (!((IntegerColumnStatistics)columnStatistics).isSumDefined()) {
                            pruningResult.addNotAgg(index);
                        }
                    }
                }
            }
        }
    }

    @Nullable
    private Set<String> getFilterSet(ExecutionContext executionContext) {
        Set<String> filterSet = null;
        String fileListStr = executionContext.getParamManager().getString(ConnectionParams.FILE_LIST);
        if (!fileListStr.equalsIgnoreCase("ALL")) {
            filterSet = Arrays.stream(fileListStr.split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
        }

        if (this.designatedFile != null) {
            if (filterSet != null) {
                filterSet = ImmutableSet.<String>builder().addAll(filterSet).add(designatedFile).build();
            } else {
                filterSet = ImmutableSet.of(designatedFile);
            }
        }

        return filterSet;
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

}
