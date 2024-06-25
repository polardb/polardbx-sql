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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * INSERT IGNORE on sharding table
 *
 * @author chenmo.cm
 */
public class LogicalInsertIgnore extends LogicalInsert {

    /**
     * Information of primary key, unique key and unique gsi.
     * For duplicate row check
     */
    protected final List<List<String>> ukColumnNamesList;
    protected final List<List<Integer>> afterUkMapping;
    protected final List<List<Integer>> beforeUkMapping;
    protected final List<Integer> afterUgsiUkIndex;
    protected final List<Integer> selectInsertColumnMapping;
    protected final List<String> pkColumnNames;
    protected final List<Integer> beforePkMapping;
    protected final List<Integer> afterPkMapping;
    protected final List<String> selectListForDuplicateCheck;
    protected final Set<String> allUkSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    // tableUkMap contains each tables' uk
    // map[tableName, map[generatedIndexKey, set[indexColumnName]]]
    protected final Map<String, Map<String, Set<String>>> tableUkMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // ukGroupByTable contains information that each UK should be looked up on which table
    // map[tableName, list[UK]]
    protected final Map<String, List<List<String>>> ukGroupByTable = new HashMap<>();

    // localIndexPhyName contains physical index name used in ukGroupByTable
    // map[tableName, list[LocalIndexName]]
    protected final Map<String, List<String>> localIndexPhyName = new HashMap<>();

    // All columns in UK and ColumnMeta
    protected final Map<String, ColumnMeta> columnMetaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    protected boolean ukContainGeneratedColumn = false;

    protected boolean usePartFieldChecker = false;

    /**
     * the target/source tables status when scaleout
     */

    protected boolean targetTableIsWritable;
    protected boolean targetTableIsReadyToPublish;
    protected boolean sourceTablesIsReadyToPublish;

    protected final List<ColumnMeta> rowColumnMetaList;
    protected final List<ColumnMeta> tableColumnMetaList;

    public LogicalInsertIgnore(LogicalInsert insert, List<String> selectListForDuplicateCheck) {
        this(insert.getCluster(),
            insert.getTraitSet(),
            insert.getTable(),
            insert.getCatalogReader(),
            insert.getInput(),
            Operation.INSERT,
            insert.isFlattened(),
            insert.getInsertRowType(),
            insert.getKeywords(),
            ImmutableList.of(),
            insert.getBatchSize(),
            insert.getAppendedColumnIndex(),
            insert.getHints(),
            insert.getTableInfo(),
            insert.getPrimaryInsertWriter(),
            insert.getGsiInsertWriters(),
            insert.getAutoIncParamIndex(),
            selectListForDuplicateCheck,
            initColumnMeta(insert),
            initTableColumnMeta(insert),
            insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList(),
            insert.getPushDownInsertWriter(),
            insert.getGsiInsertIgnoreWriters(),
            insert.getPrimaryDeleteWriter(),
            insert.getGsiDeleteWriters(),
            insert.getEvalRowColMetas(),
            insert.getGenColRexNodes(),
            insert.getInputToEvalFieldsMapping(),
            insert.getDefaultExprColMetas(),
            insert.getDefaultExprColRexNodes(),
            insert.getDefaultExprEvalFieldsMapping(),
            insert.isPushablePrimaryKeyCheck(),
            insert.isPushableForeignConstraintCheck(),
            insert.isModifyForeignKey(),
            insert.isUkContainsAllSkAndGsiContainsAllUk()
        );
    }

    public LogicalInsertIgnore(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                               Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
                               boolean flattened, RelDataType insertRowType, List<String> keywords,
                               List<RexNode> duplicateKeyUpdateList, int batchSize, Set<Integer> appendedColumnIndex,
                               SqlNodeList hints, TableInfo tableInfo, InsertWriter primaryInsertWriter,
                               List<InsertWriter> gsiInsertWriters, List<Integer> autoIncParamIndex,
                               List<String> selectListForDuplicateCheck, List<ColumnMeta> rowColumnMetas,
                               List<ColumnMeta> tableColumnMetas, LogicalDynamicValues logicalDynamicValues,
                               List<RexNode> unOpitimizedDuplicateKeyUpdateList, InsertWriter pushDownInsertWriter,
                               List<InsertWriter> gsiInsertIgnoreWriters, DistinctWriter primaryDeleteWriter,
                               List<DistinctWriter> gsiDeleteWriters, List<ColumnMeta> evalRowColMetas,
                               List<RexNode> genColRexNodes, List<Integer> inputToEvalFieldsMapping,
                               List<ColumnMeta> defaultExprColMetas, List<RexNode> defaultExprColRexNodes,
                               List<Integer> defaultExprEvalFieldsMapping, boolean pushablePrimaryKeyCheck,
                               boolean pushableForeignConstraintCheck, boolean modifyForeignKey,
                               boolean ukContainsAllSkAndGsiContainsAllUk) {
        super(cluster, traitSet, table, catalogReader, input, operation, flattened, insertRowType, keywords,
            duplicateKeyUpdateList, batchSize, appendedColumnIndex, hints, tableInfo, primaryInsertWriter,
            gsiInsertWriters, autoIncParamIndex, logicalDynamicValues, unOpitimizedDuplicateKeyUpdateList,
            evalRowColMetas, genColRexNodes, inputToEvalFieldsMapping, defaultExprColMetas, defaultExprColRexNodes,
            defaultExprEvalFieldsMapping, pushablePrimaryKeyCheck, pushableForeignConstraintCheck, modifyForeignKey,
            ukContainsAllSkAndGsiContainsAllUk);
        ExecutionContext ec = PlannerContext.getPlannerContext(cluster).getExecutionContext();

        // Ignore DELETE_ONLY UK
        this.ukColumnNamesList = GlobalIndexMeta.getUniqueKeys(getLogicalTableName(), getSchemaName(), true,
            tm -> GlobalIndexMeta.canWrite(ec, tm), ec);
        this.beforeUkMapping = initBeforeUkMapping(this.ukColumnNamesList, selectListForDuplicateCheck);
        this.afterUkMapping = initAfterUkMapping(this.ukColumnNamesList, insertRowType.getFieldNames());
        this.afterUgsiUkIndex = initAfterUgsiUkMapping(this.ukColumnNamesList, ec);
        this.selectInsertColumnMapping =
            initSelectInsertRowMapping(selectListForDuplicateCheck, insertRowType.getFieldNames());
        this.pkColumnNames = GlobalIndexMeta.getPrimaryKeys(getLogicalTableName(), getSchemaName(), ec);
        this.beforePkMapping = IntStream.range(0, pkColumnNames.size()).boxed().collect(Collectors.toList());
        this.afterPkMapping = getColumnMapping(this.pkColumnNames, insertRowType.getFieldNames());
        this.tableUkMap.putAll(GlobalIndexMeta.getTableUkMap(getLogicalTableName(), getSchemaName(), true, ec));
        this.tableUkMap.forEach((t, ukMap) -> this.allUkSet.addAll(ukMap.keySet()));
        this.rowColumnMetaList = rowColumnMetas;
        this.selectListForDuplicateCheck = selectListForDuplicateCheck;
        this.tableColumnMetaList = tableColumnMetas;
        this.pushDownInsertWriter = pushDownInsertWriter;
        this.gsiInsertIgnoreWriters = gsiInsertIgnoreWriters;
        this.primaryDeleteWriter = primaryDeleteWriter;
        this.gsiDeleteWriters = gsiDeleteWriters;
    }

    public LogicalInsertIgnore(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                               Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
                               boolean flattened, RelDataType insertRowType, List<String> keywords,
                               List<RexNode> duplicateKeyUpdateList, int batchSize, Set<Integer> appendedColumnIndex,
                               SqlNodeList hints, TableInfo tableInfo, InsertWriter primaryInsertWriter,
                               List<InsertWriter> gsiInsertWriters, List<Integer> autoIncParamIndex,
                               List<List<String>> ukColumnNamesList, List<List<Integer>> beforeUkMapping,
                               List<List<Integer>> afterUkMapping, List<Integer> afterUgsiUkIndex,
                               List<Integer> selectInsertColumnMapping, List<String> pkColumnNames,
                               List<Integer> beforePkMapping, List<Integer> afterPkMapping, Set<String> allUkSet,
                               Map<String, Map<String, Set<String>>> tableUkMap,
                               Map<String, List<List<String>>> ukGroupByTable,
                               Map<String, List<String>> localIndexPhyName, List<ColumnMeta> rowColumnMetas,
                               List<ColumnMeta> tableColumnMetas, List<String> selectListForDuplicateCheck,
                               boolean targetTableIsWritable, boolean targetTableIsReadyToPublish,
                               boolean sourceTablesIsReadyToPublish, LogicalDynamicValues logicalDynamicValues,
                               List<RexNode> unOpitimizedDuplicateKeyUpdateList, InsertWriter pushDownInsertWriter,
                               List<InsertWriter> gsiInsertIgnoreWriters, DistinctWriter primaryDeleteWriter,
                               List<DistinctWriter> gsiDeleteWriters, boolean usePartFieldChecker,
                               Map<String, ColumnMeta> columnMetaMap, boolean ukContainGeneratedColumn,
                               List<ColumnMeta> evalRowColMetas, List<RexNode> genColRexNodes,
                               List<Integer> inputToEvalFieldsMapping, List<ColumnMeta> defaultExprColMetas,
                               List<RexNode> defaultExprColRexNodes, List<Integer> defaultExprEvalFieldsMapping,
                               boolean pushablePrimaryKeyCheck, boolean pushableForeignConstraintCheck,
                               boolean modifyForeignKey, boolean ukContainsAllSkAndGsiContainsAllUk) {
        super(cluster, traitSet, table, catalogReader, input, operation, flattened, insertRowType, keywords,
            duplicateKeyUpdateList, batchSize, appendedColumnIndex, hints, tableInfo, primaryInsertWriter,
            gsiInsertWriters, autoIncParamIndex, logicalDynamicValues, unOpitimizedDuplicateKeyUpdateList,
            evalRowColMetas, genColRexNodes, inputToEvalFieldsMapping, defaultExprColMetas, defaultExprColRexNodes,
            defaultExprEvalFieldsMapping, pushablePrimaryKeyCheck, pushableForeignConstraintCheck, modifyForeignKey,
            ukContainsAllSkAndGsiContainsAllUk);

        this.ukColumnNamesList = ukColumnNamesList;
        this.beforeUkMapping = beforeUkMapping;
        this.afterUkMapping = afterUkMapping;
        this.afterUgsiUkIndex = afterUgsiUkIndex;
        this.selectInsertColumnMapping = selectInsertColumnMapping;
        this.pkColumnNames = pkColumnNames;
        this.beforePkMapping = beforePkMapping;
        this.afterPkMapping = afterPkMapping;
        this.allUkSet.addAll(allUkSet);
        this.tableUkMap.putAll(tableUkMap);
        this.ukGroupByTable.putAll(ukGroupByTable);
        this.localIndexPhyName.putAll(localIndexPhyName);
        this.rowColumnMetaList = rowColumnMetas;
        this.tableColumnMetaList = tableColumnMetas;
        this.selectListForDuplicateCheck = selectListForDuplicateCheck;
        this.targetTableIsWritable = targetTableIsWritable;
        this.targetTableIsReadyToPublish = targetTableIsReadyToPublish;
        this.sourceTablesIsReadyToPublish = sourceTablesIsReadyToPublish;
        this.pushDownInsertWriter = pushDownInsertWriter;
        this.gsiInsertIgnoreWriters = gsiInsertIgnoreWriters;
        this.primaryDeleteWriter = primaryDeleteWriter;
        this.gsiDeleteWriters = gsiDeleteWriters;
        this.usePartFieldChecker = usePartFieldChecker;
        this.columnMetaMap.putAll(columnMetaMap);
        this.ukContainGeneratedColumn = ukContainGeneratedColumn;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        LogicalInsertIgnore newInsertIgnore = new LogicalInsertIgnore(getCluster(),
            traitSet,
            getTable(),
            getCatalogReader(),
            sole(inputs),
            getOperation(),
            isFlattened(),
            getInsertRowType(),
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getPrimaryInsertWriter(),
            getGsiInsertWriters(),
            getAutoIncParamIndex(),
            getUkColumnNamesList(),
            getBeforeUkMapping(),
            getAfterUkMapping(),
            getAfterUgsiUkIndex(),
            getSelectInsertColumnMapping(),
            getPkColumnNames(),
            getBeforePkMapping(),
            getAfterPkMapping(),
            getAllUkSet(),
            getTableUkMap(),
            getUkGroupByTable(),
            getLocalIndexPhyName(),
            getRowColumnMetaList(),
            getTableColumnMetaList(),
            getSelectListForDuplicateCheck(),
            isTargetTableIsWritable(),
            isTargetTableIsReadyToPublish(),
            isSourceTablesIsReadyToPublish(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getPushDownInsertWriter(),
            getGsiInsertIgnoreWriters(),
            getPrimaryDeleteWriter(),
            getGsiDeleteWriters(),
            isUsePartFieldChecker(),
            getColumnMetaMap(),
            isUkContainGeneratedColumn(),
            getEvalRowColMetas(),
            getGenColRexNodes(),
            getInputToEvalFieldsMapping(),
            getDefaultExprColMetas(),
            getDefaultExprColRexNodes(),
            getDefaultExprEvalFieldsMapping(),
            isPushablePrimaryKeyCheck(),
            isPushableForeignConstraintCheck(),
            isModifyForeignKey(),
            isUkContainsAllSkAndGsiContainsAllUk()
        );
        return newInsertIgnore;
    }

    protected static List<ColumnMeta> initColumnMeta(LogicalInsert insert) {
        if (insert instanceof LogicalInsertIgnore) {
            return ((LogicalInsertIgnore) insert).getRowColumnMetaList();
        }

        final TableMeta table =
            PlannerContext.getPlannerContext(insert).getExecutionContext().getSchemaManager(insert.getSchemaName())
                .getTable(insert.getLogicalTableName());
        final List<String> fieldNames = insert.getInsertRowType().getFieldNames();
        return fieldNames.stream().map(table::getColumn).collect(Collectors.toList());
    }

    protected static List<ColumnMeta> initTableColumnMeta(LogicalInsert insert) {
        if (insert instanceof LogicalInsertIgnore) {
            return ((LogicalInsertIgnore) insert).getTableColumnMetaList();
        }

        final TableMeta table =
            PlannerContext.getPlannerContext(insert).getExecutionContext().getSchemaManager(insert.getSchemaName())
                .getTable(insert.getLogicalTableName());

        final List<String> fieldNames = insert.getTable().getRowType().getFieldNames();
        return fieldNames.stream().map(table::getColumn).collect(Collectors.toList());
    }

    public List<ColumnMeta> getRowColumnMetaList() {
        return rowColumnMetaList;
    }

    public List<ColumnMeta> getTableColumnMetaList() {
        return tableColumnMetaList;
    }

    public List<List<ColumnMeta>> getUkColumnMetas() {
        return initUkColumnMeta(getAfterUkMapping(), getRowColumnMetaList());
    }

    public List<ColumnMeta> getPkColumnMetas() {
        final List<ColumnMeta> rowColumnMetaList = getRowColumnMetaList();
        return getAfterPkMapping().stream().map(rowColumnMetaList::get).collect(Collectors.toList());
    }

    protected static List<List<ColumnMeta>> initUkColumnMeta(List<List<Integer>> ukColumnsList,
                                                             List<ColumnMeta> rowColumnMeta) {
        final List<List<ColumnMeta>> ukColumnMetas = new ArrayList<>();
        for (List<Integer> ukColumns : ukColumnsList) {
            ukColumnMetas.add(ukColumns.stream().map(rowColumnMeta::get).collect(Collectors.toList()));
        }
        return ukColumnMetas;
    }

    public List<Integer> initAfterUgsiUkMapping(List<List<String>> ukList, ExecutionContext ec) {
        String schema = getSchemaName();
        String table = getLogicalTableName();

        List<TableMeta> ugsiMetas = GlobalIndexMeta.getIndex(table, schema, ec).stream()
            .filter(tm -> GlobalIndexMeta.canWrite(ec, tm) && (tm.hasGsiImplicitPrimaryKey()))
            .collect(Collectors.toList());

        List<List<String>> ugsiUkList = new ArrayList<>();
        for (TableMeta ugsiMeta : ugsiMetas) {
            ugsiUkList.add(ugsiMeta.getGsiTableMetaBean().gsiMetaBean.indexColumns.stream().map(cmb -> cmb.columnName)
                .collect(Collectors.toList()));
        }

        List<Set<String>> ukSet = ukList.stream().map(l -> {
            Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            set.addAll(l);
            return set;
        }).collect(Collectors.toList());

        List<Integer> results = new ArrayList<>();
        for (List<String> ugsiUk : ugsiUkList) {
            for (int i = 0; i < ukSet.size(); i++) {
                if (ukSet.get(i).size() == ugsiUk.size() && ukSet.get(i).containsAll(ugsiUk)) {
                    results.add(i);
                    break;
                }
            }
        }

        return results;
    }

    public static List<List<Integer>> initAfterUkMapping(List<List<String>> ukColumnNamesList,
                                                         List<String> fieldNames) {
        return getUkColumnMapping(ukColumnNamesList, fieldNames);
    }

    public static List<List<Integer>> initBeforeUkMapping(List<List<String>> ukColumnNamesList,
                                                          List<String> selectListForDuplicateCheck) {
        return getUkColumnMapping(ukColumnNamesList, selectListForDuplicateCheck);
    }

    public static List<Integer> initSelectInsertRowMapping(List<String> selectColumns, List<String> insertColumns) {
        // selectColumns must be a subset of insertColumns
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < selectColumns.size(); i++) {
            int pos = -1;
            for (int j = 0; j < insertColumns.size(); j++) {
                if (selectColumns.get(i).equalsIgnoreCase(insertColumns.get(j))) {
                    pos = j;
                    break;
                }
            }
            result.add(pos);
        }
        return result;
    }

    private static List<List<Integer>> getUkColumnMapping(List<List<String>> ukColumnNamesList,
                                                          List<String> inputColumnNames) {
        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(inputColumnNames).forEach(o -> columnIndexMap.put(o.getValue(), o.getKey()));
        return ukColumnNamesList.stream()
            .map(columnNames -> columnNames.stream().map(columnIndexMap::get).filter(Objects::nonNull)
                .collect(Collectors.toList()))
            .filter(cl -> !cl.isEmpty())
            .collect(Collectors.toList());
    }

    private static List<Integer> getColumnMapping(List<String> columnNames, List<String> inputColumnNames) {
        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(inputColumnNames).forEach(o -> columnIndexMap.put(o.getValue(), o.getKey()));
        return columnNames.stream().map(columnIndexMap::get).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    protected <R extends LogicalInsert> List<RelNode> getPhyPlanForDisplay(ExecutionContext executionContext,
                                                                           R insertIgnore) {
        final InsertWriter primaryWriter = getPrimaryInsertWriter();
        final LogicalInsert insert = primaryWriter.getInsert();
        final Set<String> keywords = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (GeneralUtil.isNotEmpty(insert.getKeywords())) {
            keywords.addAll(insert.getKeywords());
        }
        keywords.add("IGNORE");
        final LogicalInsert copied = new LogicalInsert(insert.getCluster(), insert.getTraitSet(), insert.getTable(),
            insert.getCatalogReader(), insert.getInput(), insert.getOperation(), insert.isFlattened(),
            insert.getInsertRowType(), Lists.newArrayList(keywords), insert.getDuplicateKeyUpdateList(),
            insert.getBatchSize(), insert.getAppendedColumnIndex(), insert.getHints(), insert.getTableInfo(), null,
            new ArrayList<>(), insert.getAutoIncParamIndex(), insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList(), insert.getEvalRowColMetas(), insert.getGenColRexNodes(),
            insert.getInputToEvalFieldsMapping(), insert.getDefaultExprColMetas(), insert.getDefaultExprColRexNodes(),
            insert.getDefaultExprEvalFieldsMapping(), insert.isPushablePrimaryKeyCheck(),
            insert.isPushableForeignConstraintCheck(), insert.isModifyForeignKey(),
            insert.isUkContainsAllSkAndGsiContainsAllUk());

        final InsertWriter insertIgnoreWriter = new InsertWriter(primaryWriter.getTargetTable(), copied);
        return insertIgnoreWriter.getInput(executionContext);
    }

    @Override
    public boolean withWriter() {
        return true;
    }

    public boolean withoutPkAndUk() {
        return GeneralUtil.isEmpty(ukColumnNamesList) || ukColumnNamesList.get(0).isEmpty();
    }

    public boolean isSourceTablesIsReadyToPublish() {
        return sourceTablesIsReadyToPublish;
    }

    public void setSourceTablesIsReadyToPublish(boolean sourceTablesIsReadyToPublish) {
        this.sourceTablesIsReadyToPublish = sourceTablesIsReadyToPublish;
    }

    public boolean isTargetTableIsReadyToPublish() {
        return targetTableIsReadyToPublish;
    }

    public void setTargetTableIsReadyToPublish(boolean targetTableIsReadyToPublish) {
        this.targetTableIsReadyToPublish = targetTableIsReadyToPublish;
    }

    public boolean isTargetTableIsWritable() {
        return targetTableIsWritable;
    }

    public void setTargetTableIsWritable(boolean targetTableIsWritable) {
        this.targetTableIsWritable = targetTableIsWritable;
    }

    public boolean containsAllUk(String tableName) {
        Map<String, Set<String>> ukMap = getTableUkMap().get(tableName);
        if (ukMap == null) {
            return false;
        }
        return ukMap.keySet().containsAll(getAllUkSet());
    }

    @Override
    public String explainNodeName() {
        if (isReplace()) {
            return "LogicalReplace";
        } else if (withDuplicateKeyUpdate()) {
            return "LogicalUpsert";
        }
        return "LogicalInsertIgnore";
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
        final boolean isSourceSelect = isSourceSelect();
        if (isSourceSelect) {
            pw.item("table", getLogicalTableName());
            pw.item("columns", getInsertRowType());
        } else {
            pw.item("sql", getSqlTemplate().toString().replace("\n", " "));
        }
        pw.item("uniqueKeySelect",
            ukGroupByTable.entrySet().stream().map(e -> "select " + e.getValue() + " on " + e.getKey())
                .collect(Collectors.toList()));
        if (isSourceSelect) {
            pw.item("mode", insertSelectMode);
        }
        return pw;
    }

    public List<List<String>> getUkColumnNamesList() {
        return ukColumnNamesList;
    }

    public List<List<Integer>> getBeforeUkMapping() {
        return beforeUkMapping;
    }

    public List<List<Integer>> getAfterUkMapping() {
        return afterUkMapping;
    }

    public List<Integer> getAfterUgsiUkIndex() {
        return afterUgsiUkIndex;
    }

    public List<Integer> getSelectInsertColumnMapping() {
        return selectInsertColumnMapping;
    }

    public Set<String> getAllUkSet() {
        return allUkSet;
    }

    public Map<String, Map<String, Set<String>>> getTableUkMap() {
        return tableUkMap;
    }

    public Map<String, List<List<String>>> getUkGroupByTable() {
        return ukGroupByTable;
    }

    public Map<String, List<String>> getLocalIndexPhyName() {
        return localIndexPhyName;
    }

    public List<String> getSelectListForDuplicateCheck() {
        return selectListForDuplicateCheck;
    }

    public List<String> getPkColumnNames() {
        return pkColumnNames;
    }

    public List<Integer> getBeforePkMapping() {
        return beforePkMapping;
    }

    public List<Integer> getAfterPkMapping() {
        return afterPkMapping;
    }

    public boolean isUsePartFieldChecker() {
        return usePartFieldChecker;
    }

    public void setUsePartFieldChecker(boolean usePartFieldChecker) {
        this.usePartFieldChecker = usePartFieldChecker;
    }

    public boolean isUkContainGeneratedColumn() {
        return ukContainGeneratedColumn;
    }

    public void setUkContainGeneratedColumn(boolean ukContainGeneratedColumn) {
        this.ukContainGeneratedColumn = ukContainGeneratedColumn;
    }

    public Map<String, ColumnMeta> getColumnMetaMap() {
        return columnMetaMap;
    }
}
