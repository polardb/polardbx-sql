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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class CandidateIndex {

    public static final String WHAT_IF_GSI_INFIX = "__what_if_gsi_";

    public static final String WHAT_IF_INDEX_INFIX = "__what_if_";

    public static final String WHAT_IF_AUTO_INDEX_INFIX = "__what_if_auto_shard_key_";

    private static final String ADVISE_GSI_PREFIX = "__advise_index_gsi_";

    private static final String ADVISE_INDEX_PREFIX = "__advise_index_";

    public static final int INDEX_LENGTH = 50;
    private String schemaName;

    private String tableName;

    private List<String> columnNames;

    private TableMeta tableMeta;

    private IndexMeta indexMeta;

    private Boolean isHighCardinality = null;

    private boolean gsi;

    private PartitionByDefinition partitionByDefinition;

    private PartitionInfo originPartitionInfo;

    private Set<String> coveringColumns;

    private List<String> dbPartitionKeys;

    private String dbPartitionPolicy;

    private Integer dbCount;

    private List<String> tbPartitionKeys;

    private String tbPartitionPolicy;

    private Integer tbCount;

    private boolean changePartitionPolicy = false;

    private boolean containGsiColOfUnsupportedDataType = false;

    private static Set<String> datePartitionPolicySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private static Set<String> specialHashPartitionPolicySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    static {
        datePartitionPolicySet.add("MM");
        datePartitionPolicySet.add("DD");
        datePartitionPolicySet.add("MMDD");
        datePartitionPolicySet.add("WEEK");
        datePartitionPolicySet.add("YYYYDD");
        datePartitionPolicySet.add("YYYYMM");
        datePartitionPolicySet.add("YYYYWEEK");

        specialHashPartitionPolicySet.add("RIGHT_SHIFT");
        specialHashPartitionPolicySet.add("UNI_HASH");
        specialHashPartitionPolicySet.add("STR_HASH");
    }

    public CandidateIndex(String schemaName, String tableName, List<String> columnNames,
                          boolean gsi, Set<String> coveringColumns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.gsi = gsi;
        if (coveringColumns == null) {
            coveringColumns = new HashSet<>();
        }
        this.coveringColumns = coveringColumns.stream().filter(
            name -> !columnNames.stream().map(x -> x.toLowerCase()).collect(Collectors.toSet())
                .contains(name.toLowerCase())
        ).collect(Collectors.toSet());

        if (!gsi) {
            return;
        }

        // default gsi partition policy
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        if (partitionInfoManager.isNewPartDbTable(tableName)) {
//            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
//            PartitionInfo tmpPartInfo = partitionInfo.copy();
//            PartitionByDefinition partitionByDefinition = tmpPartInfo.getPartitionBy();
//            this.partitionByDefinition = partitionByDefinition.copy();
//
//            // use column as default partition expr
//            List<SqlNode> partitionExprList =
//                columnNames.stream().map(x -> new SqlIdentifier(x, SqlParserPos.ZERO)).collect(Collectors.toList());
//            this.partitionByDefinition.setPartitionExprList(partitionExprList);
//            this.partitionByDefinition.setPartitionColumnNameList(columnNames);
//            this.partitionByDefinition.setPartitionFieldList(
//                columnNames.stream().map(x -> getTableMeta().getColumnIgnoreCase(x)).collect(Collectors.toList()));
//
//            // use KEY partition as default partition
//            this.partitionByDefinition.setStrategy(PartitionStrategy.KEY);
//            this.partitionByDefinition.setPartitions(new ArrayList<>());
            List<String> newGsiAllCols = new ArrayList<>();
            newGsiAllCols.addAll(columnNames);
            newGsiAllCols.addAll(coveringColumns);
            TableMeta primaryTblMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            long hashPartCnt = primaryTblMeta.getPartitionInfo().getPartitionBy().getPartitions().size();
            PartitionInfo newGsiPartInfo =
                PartitionInfoBuilder.buildPartInfoWithKeyStrategyForNewGsiMeta(schemaName, tableName, primaryTblMeta,
                    newGsiAllCols, columnNames, hashPartCnt);
            this.partitionByDefinition = newGsiPartInfo.getPartitionBy();

            // clear dbpartition and tbpartition
            this.dbCount = 0;
            this.dbPartitionKeys = new ArrayList<>();
            this.dbPartitionPolicy = null;
            this.tbCount = 0;
            this.tbPartitionKeys = new ArrayList<>();
            this.tbPartitionPolicy = null;
        } else {
            // default partition policy
            if (DataTypeUtil.isDateType(getIndexMeta().getKeyColumns().get(0).getDataType())) {
                this.dbPartitionPolicy = "YYYYDD";
            } else {
                this.dbPartitionPolicy = "HASH";
            }
            this.dbPartitionKeys = new ArrayList<>();
            this.dbPartitionKeys.add(columnNames.get(0));
            TableRule rule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tableName);
            if (rule == null) {
                this.dbCount = 1;
            } else {
                this.dbCount = rule.getActualTopology().size();
            }
            this.tbPartitionKeys = new ArrayList<>();
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public boolean isContainGsiPartColOfUnsupportedDataType() {
        return containGsiColOfUnsupportedDataType;
    }

    public void setContainGsiColOfUnsupportedDataType(boolean containGsiColOfUnsupportedDataType) {
        this.containGsiColOfUnsupportedDataType = containGsiColOfUnsupportedDataType;
    }

    public TableMeta getTableMeta() {
        if (tableMeta != null) {
            return tableMeta;
        }
        return tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
    }

    public IndexMeta getIndexMeta() {
        if (indexMeta != null) {
            return indexMeta;
        }

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        List<ColumnMeta> keys = columnNames.stream().map(columnName -> tableMeta.getColumn(columnName)).collect(
            Collectors.toList());
        List<ColumnMeta> values = new ArrayList<>();
        IndexMeta whatIfIndex = new IndexMeta(tableName, keys, values, IndexType.BTREE, Relationship.NONE, true,
            false, false,
            getLocalIndexName());
        return indexMeta = whatIfIndex;
    }

    private String getLocalIndexName() {
        return tableName + WHAT_IF_INDEX_INFIX + String.join("_", columnNames);
    }

    public String getIndexName() {
        if (gsi) {
            return tableName + WHAT_IF_GSI_INFIX + String.join("_", columnNames);
        } else {
            return getLocalIndexName();
        }
    }

    public String getIndexNameForUser() {
        if (gsi) {
            return ADVISE_GSI_PREFIX + tableName + "_" + String.join("_", columnNames);
        } else {
            return ADVISE_INDEX_PREFIX + tableName + "_" + String.join("_", columnNames);
        }
    }

    public String getSql() {
        StringBuilder sql = new StringBuilder("ALTER TABLE `");

        String indexNameForUser = getIndexNameForUser();

        // for gsi, the index table will add suffix "_$xxxx" automatically
        sql.append(schemaName)
            .append("`.`").append(tableName).append("` ADD ").append(gsi ? "GLOBAL" : "").append(" INDEX `")
            .append(indexNameForUser.substring(0, Math.min(INDEX_LENGTH, indexNameForUser.length()))).append("`(")
            .append(String.join(",",
                columnNames.stream().map(name -> "`" + name + "`").collect(Collectors.toList())))
            .append(")");

        if (gsi) {
            if (coveringColumns != null && !coveringColumns.isEmpty()) {
                sql.append(" COVERING(")
                    .append(String.join(",",
                        coveringColumns.stream().map(name -> "`" + name + "`").collect(Collectors.toList())))
                    .append(")");
            }

            if (partitionByDefinition != null) {
                sql.append(" ").append(partitionByDefinition.toString());
            } else {
                sql.append(" DBPARTITION BY ").append(getDbPartitionPolicy().toUpperCase());
                if (getDbPartitionPolicy().indexOf("(") != -1) {
                    // right_shift(`name`, 8)
                } else {
                    sql.append("(").append(getDbPartitionKey()).append(")");
                }

                if (getTbPartitionPolicy() != null) {
                    sql.append(" TBPARTITION BY ").append(getTbPartitionPolicy().toUpperCase());
                    if (getTbPartitionPolicy().indexOf("(") != -1) {
                        // right_shift(`name`, 8)
                    } else {
                        sql.append("(").append(getTbPartitionKey()).append(")");
                    }

                    if (getTbPartitionCount() != null) {
                        sql.append(" TBPARTITIONS ").append(getTbPartitionCount());
                    }
                }
            }
        }

        return sql.toString();
    }

    public boolean isHighCardinality() {

        if (isHighCardinality != null) {
            return isHighCardinality.booleanValue();
        }

        long indexCardinality = 1;
        for (String columnName : columnNames) {
            StatisticResult statisticResult =
                StatisticManager.getInstance().getCardinality(schemaName, tableName, columnName, true, false);
            long cardinality = statisticResult.getLongValue();
            if (cardinality <= 0) {
                return false;
            }
            indexCardinality *= cardinality;
        }

        // reject low cardinality sharding key for gsi
        if (isGsi() || getTableMeta().isAutoPartition()) {
            String columnName = columnNames.get(0);
            StatisticResult statisticResult =
                StatisticManager.getInstance().getCardinality(schemaName, tableName, columnName, true, false);
            long cardinality = statisticResult.getLongValue();

            long partitions;
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                partitions = OptimizerContext.getContext(schemaName).
                    getLatestSchemaManager().getTddlRuleManager().getPartitionInfoManager().getPartitionInfo(tableName)
                    .getPartitionBy().getPartitions().size();
            } else {
                partitions = OptimizerContext.getContext(schemaName).
                    getLatestSchemaManager().getTddlRuleManager().getTableRule(tableName).getActualDbCount();
            }
            int base = Math.min(1, InstConfUtil.getInt(ConnectionParams.INDEX_ADVISOR_CARDINALITY_BASE));
            return isHighCardinality = cardinality >= partitions * base;
        }
        StatisticResult statisticResult = StatisticManager.getInstance().getRowCount(schemaName, tableName, false);
        long rowCount = statisticResult.getLongValue();
        return isHighCardinality = indexCardinality > rowCount * 0.0000001;
    }

    public boolean isNotCoverPrimaryUniqueKey() {
        TableMeta tableMeta = getTableMeta();
        List<IndexMeta> uniqueKeyList = tableMeta.getUniqueIndexes(true);
        for (IndexMeta uniqueKey : uniqueKeyList) {
            if (isIndexCover(uniqueKey)) {
                return false;
            }
        }
        return true;
    }

    private boolean isIndexCover(IndexMeta index) {
        // whatIfIndex cover index
        for (ColumnMeta columnMeta : index.getKeyColumns()) {
            boolean found = false;
            for (String whatIfColumn : columnNames) {
                if (whatIfColumn.equalsIgnoreCase(columnMeta.getName())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    public boolean alreadyExist() {
        TableMeta tableMeta = getTableMeta();
        if (gsi) {
            String digest1 = columnNames.stream().reduce("", (a, b) -> a + "," + b);
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiMap = tableMeta.getGsiPublished();
            if (gsiMap != null) {
                for (GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean : gsiMap.values()) {
                    String digest2 = gsiIndexMetaBean.indexColumns.stream().map(x -> x.columnName).reduce("",
                        (a, b) -> a + "," + b);
                    if (digest1.equalsIgnoreCase(digest2)) {
                        if (coveringColumns != null && !coveringColumns.isEmpty()) {
                            Set<String> existGsiCoveringColumnSet =
                                gsiIndexMetaBean.coveringColumns.stream().map(x -> x.columnName.toLowerCase())
                                    .collect(Collectors.toSet());
                            Set<String> candidateGsiCoveringColumnSet =
                                coveringColumns.stream().map(x -> x.toLowerCase()).collect(Collectors.toSet());
                            if (existGsiCoveringColumnSet.containsAll(candidateGsiCoveringColumnSet)) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                }
                // TODO: check covering column
                // TODO: check partition rule
            }
        } else {
            IndexMeta whatIfIndexMeta = getIndexMeta();
            for (IndexMeta indexMeta : tableMeta.getIndexes()) {
                if (indexMeta.getName().equalsIgnoreCase(whatIfIndexMeta.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isGsi() {
        return gsi;
    }

    public Set<String> getCoveringColumns() {
        return coveringColumns;
    }

    public String getDbPartitionKey() {
        return String.join(",", dbPartitionKeys.stream().map(x -> "`" + x + "`").collect(Collectors.toList()));
    }

    public String getDbPartitionPolicy() {
        return dbPartitionPolicy;
    }

    public Integer getDbPartitionCount() {
        return dbCount;
    }

    public String getTbPartitionKey() {
        return String.join(",", tbPartitionKeys.stream().map(x -> "`" + x + "`").collect(Collectors.toList()));
    }

    public String getTbPartitionPolicy() {
        return tbPartitionPolicy;
    }

    public Integer getTbPartitionCount() {
        return tbCount;
    }

    public boolean notSupportPartitionGsi() {
        DataType dataType = getIndexMeta().getKeyColumns().get(0).getDataType();
        return dataTypeNotSupportPartitionGsi(dataType);
    }

    private boolean dataTypeNotSupportPartitionGsi(DataType dataType) {
        return isGsi() && dataTypeNotSupportPartitionType(dataType);
    }

    private boolean dataTypeNotSupportPartitionType(DataType dataType) {
        return DataTypeUtil.equalsSemantically(DataTypes.YearType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.FloatType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.DoubleType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.DecimalType, dataType);
    }

    public boolean changeByPartitionInfo(PartitionInfo partitionInfo) {

        if (partitionInfo.getPartitionBy() == null) {
            return false;
        }

        // FIXME: support sub partition
        if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            return false;
        }

        if (partitionInfo.isBroadcastTable() || partitionInfo.isSingleTable()) {
            return false;
        }

        // GSI must contain enough column for partition key
        Set<String> partitionKeySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        partitionKeySet.addAll(partitionInfo.getPartitionColumns());
        if (columnNames.size() < partitionKeySet.size()) {
            return false;
        }
        // GSI table should not have gsi
        if (getTableMeta().isGsi()) {
            return false;
        }

        PartitionByDefinition partitionByDefinition = partitionInfo.getPartitionBy();
        if (!partitionByDefinition.getPartitionExprList().stream().allMatch(x -> x instanceof SqlIdentifier)) {
            // only support simple expr
            return false;
        }

        // TODO check whether type and partition method are consist
        switch (partitionByDefinition.getStrategy()) {
        case HASH:
            if (columnNames.size() > 1) {
                return false;
            } else if (DataTypeUtil.isUnderBigintType(getIndexMeta().getKeyColumns().get(0).getDataType())) {
                // pass
            } else {
                return false;
            }
            break;
        case KEY:
            break;
        case RANGE:
        case RANGE_COLUMNS:
        case LIST_COLUMNS:
        default:
            return false;
        }

        this.originPartitionInfo = partitionInfo;

//        this.partitionByDefinition = partitionByDefinition.copy();
//        List<SqlNode> partitionExprList =
//            columnNames.stream().map(x -> new SqlIdentifier(x, SqlParserPos.ZERO)).collect(Collectors.toList());
//        this.partitionByDefinition.setPartitionExprList(partitionExprList);
//        this.partitionByDefinition.setPartitionColumnNameList(columnNames);
//        this.partitionByDefinition.setPartitionFieldList(
//            columnNames.stream().map(x -> getTableMeta().getColumnIgnoreCase(x)).collect(Collectors.toList()));

        // clear dbpartition and tbpartition
        this.dbCount = 0;
        this.dbPartitionKeys = new ArrayList<>();
        this.dbPartitionPolicy = null;
        this.tbCount = 0;
        this.tbPartitionKeys = new ArrayList<>();
        this.tbPartitionPolicy = null;

        return true;
    }

    public boolean changePartitionPolicy(HumanReadableRule rule) {
        if (rule.isSingle() || rule.isBroadcast()) {
            return false;
        }

        if (rule.isPartition()) {
            return changePartitionPolicy = changeByPartitionInfo(rule.getPartitionInfo());
        }

        // GSI do not support partition without dbPartition
        if (rule.dbPartitionKeys == null || rule.dbPartitionKeys.isEmpty()) {
            return false;
        }

        // GSI must contain enough column for partition key
        Set<String> partitionKeySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        partitionKeySet.addAll(rule.dbPartitionKeys);
        if (rule.tbPartitionKeys != null) {
            partitionKeySet.addAll(rule.tbPartitionKeys);
        }
        if (columnNames.size() < partitionKeySet.size()) {
            return false;
        }
        // GSI table should not have gsi
        if (getTableMeta().isGsi()) {
            return false;
        }

        // change here
        List<String> dpkeys = new ArrayList<>();
        List<String> tpkeys = new ArrayList<>();
        for (int i = 0; i < rule.dbPartitionKeys.size(); i++) {
            dpkeys.add(columnNames.get(i));
        }
        if (rule.tbPartitionKeys != null) {
            int idx = rule.tbPartitionKeys.size();
            for (int i = 0; i < rule.tbPartitionKeys.size(); i++) {
                String columnToAdd = null;
                String key = rule.tbPartitionKeys.get(i);
                boolean found = false;
                for (int j = 0; j < rule.dbPartitionKeys.size(); j++) {
                    if (rule.dbPartitionKeys.get(j).equalsIgnoreCase(key)) {
                        columnToAdd = columnNames.get(j);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    columnToAdd = columnNames.get(idx++);
                }
                tpkeys.add(columnToAdd);
            }
        }

        // GSI shouldn't partition like the primary table
        TableRule tableRule =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTddlRuleManager()
                .getTableRule(tableName);
        HumanReadableRule primaryTableRule = HumanReadableRule.getHumanReadableRule(tableRule);
        if (primaryTableRule.getPartitionPolicyDigest().equalsIgnoreCase(rule.getPartitionPolicyDigest())) {
            if (keyListEqual(dpkeys, tableRule.getDbPartitionKeys())
                && keyListEqual(tpkeys, tableRule.getTbPartitionKeys())) {
                return false;
            }
        }

        String tmpDbPartitionPolicy = rule.dbPartitionPolicy.toLowerCase();
        for (int i = 0; i < rule.dbPartitionKeys.size(); i++) {
            tmpDbPartitionPolicy = tmpDbPartitionPolicy.replace(
                "`" + rule.dbPartitionKeys.get(i).toLowerCase() + "`", "`" + dpkeys.get(i) + "`");
        }

        String tmpTbPartitionPolicy = null;
        if (rule.tbPartitionPolicy != null) {
            tmpTbPartitionPolicy = rule.tbPartitionPolicy.toLowerCase();
        }

        if (rule.tbPartitionKeys != null) {
            for (int i = 0; i < rule.tbPartitionKeys.size(); i++) {
                tmpTbPartitionPolicy = tmpTbPartitionPolicy.replace(
                    "`" + rule.tbPartitionKeys.get(i).toLowerCase() + "`", "`" + tpkeys.get(i) + "`");
            }
        }

        // dbPartition key is date, but dbPartition Policy is not date, bail out
        if (!datePartitionPolicySet.contains(tmpDbPartitionPolicy)
            && isGsi()
            && DataTypeUtil.isDateType(getIndexMeta().getKeyColumns().get(0).getDataType())) {
            return false;
        }

        // dbPartition key is not date, but dbPartition Policy is date, bail out
        if (datePartitionPolicySet.contains(tmpDbPartitionPolicy)
            && isGsi()
            && !DataTypeUtil.isDateType(getIndexMeta().getKeyColumns().get(0).getDataType())) {
            return false;
        }

        if (tpkeys != null && !tpkeys.isEmpty() && tmpTbPartitionPolicy != null) {
            // tbPartition key is date, but tbPartition Policy is not date, bail out
            if (!datePartitionPolicySet.contains(tmpTbPartitionPolicy)
                && isGsi()
                && DataTypeUtil.isDateType(getIndexMeta().getKeyColumn(tpkeys.get(0)).getDataType())) {
                return false;
            }

            // tbPartition key is not date, but tbPartition Policy is date, bail out
            if (datePartitionPolicySet.contains(tmpTbPartitionPolicy)
                && isGsi()
                && !DataTypeUtil.isDateType(getIndexMeta().getKeyColumn(tpkeys.get(0)).getDataType())) {
                return false;
            }

            // tbPartition key type not support
            if (dataTypeNotSupportPartitionGsi(getIndexMeta().getKeyColumn(tpkeys.get(0)).getDataType())) {
                return false;
            }
        }

        // do not support RIGHT_SHIFT,UNI_HASH,STR_HASH
        for (String partitionPolicy : specialHashPartitionPolicySet) {
            if (tmpDbPartitionPolicy.toLowerCase().indexOf(partitionPolicy.toLowerCase()) != -1) {
                return false;
            }
            if (tpkeys != null && !tpkeys.isEmpty() && tmpTbPartitionPolicy != null) {
                if (tmpTbPartitionPolicy.toLowerCase().indexOf(partitionPolicy.toLowerCase()) != -1) {
                    return false;
                }
            }
        }

        this.dbPartitionKeys = dpkeys;
        this.dbPartitionPolicy = tmpDbPartitionPolicy;
        this.dbCount = rule.dbCount;

        this.tbPartitionKeys = tpkeys;
        this.tbPartitionPolicy = tmpTbPartitionPolicy;
        this.tbCount = rule.tbCount;

        return changePartitionPolicy = true;
    }

    public boolean hasChangePartitionPolicy() {
        return changePartitionPolicy;
    }

    public HumanReadableRule getHumanReadableRule() {
        HumanReadableRule rule;
        if (originPartitionInfo != null) {
            rule = new HumanReadableRule(originPartitionInfo);
        } else {
            rule = new HumanReadableRule(
                tableName, false, dbPartitionKeys, dbPartitionPolicy, dbCount,
                tbPartitionKeys, tbPartitionPolicy, tbCount);
        }
        return rule;
    }

    private boolean keyListEqual(List<String> keyList1, List<String> keyList2) {
        if (keyList1 == null || keyList1.isEmpty()) {
            return keyList2 == null || keyList2.isEmpty();
        }
        if (keyList1.size() != keyList2.size()) {
            return false;
        }
        for (int i = 0; i < keyList1.size(); i++) {
            if (!keyList1.get(i).equalsIgnoreCase(keyList2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public CandidateIndex copyAsGsi() {
        // ignore gsi for unsupported date type
        for (ColumnMeta meta : getIndexMeta().getKeyColumns()) {
            if (dataTypeNotSupportPartitionType(meta.getDataType())) {
                return null;
            }
        }
        CandidateIndex newCandidateIndex = new CandidateIndex(schemaName, tableName, columnNames, true,
            coveringColumns);
        return newCandidateIndex;
    }
}
