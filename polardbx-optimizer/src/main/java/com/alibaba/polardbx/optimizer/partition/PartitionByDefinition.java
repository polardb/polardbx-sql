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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class PartitionByDefinition {

    /**
     * the partition strategy of current partitions
     */
    protected PartitionStrategy strategy;

    /**
     * the definition of partitions
     */
    protected List<PartitionSpec> partitions;

    /**
     * The raw partition expression from create tbl ddl or meta db
     */
    protected List<SqlNode> partitionExprList;

    /**
     * The return type of raw partition expression from create tbl ddl or meta db
     * <pre>
     *     For example,  the table tbl is partitioned by "HASH(Year(gmtCreated))",
     *
     *     Although the partition column gmtCreated is the data type DATETIME,
     *     but its return type of partition  expression of "Year(gmtCreated)" is INT,
     *     and the final  data type of hashCode after doing hashing ( which is "murmur3(Year(gmtCreated))" ) is LONG,
     *
     *     So the element of partitionExprTypeList[i] is the return type of partition expression.
     *
     * </pre>
     */
    protected List<RelDataType> partitionExprTypeList;

    /**
     * list of fields used in partitioned expression
     */
    protected List<ColumnMeta> partitionFieldList;

    /**
     * list of column name of partition fields, all col name must be in lower case
     */
    protected List<String> partitionColumnNameList;

    //============== The following attribute are dynamic build when PartitionInfo is created  ==================

    /**
     *
     * The data type of PartField in different space will have different data types as followed:
     *
     * <pre>
     *
     *     PartStrategy                   Hash              Hash                    Key          Range/List      
     *
     *     PartField DataType           Datetime           BigInt Unsigned        Varchar      Bigint Unsigned
     *
     *     Use PartIntFunc                Year                 /                     /               /
     *
     *     QuerySpace DataType          Datetime           BigInt Unsigned        Varchar      Bigint Unsigned
     *
     *     PruningSpace DataType        Int                BigInt Unsigned        Varchar      BigInt Unsigned
     *     (after calculating intFunc)
     *
     *     BoundValSpace DataType       Long               Long                    Long        BigInt Unsigned
     *     (after computing hashVal)
     *
     *
     * </pre>
     *
     *
     */
    /**
     * The Comparator used to do compare in the partition query space
     */
    protected SearchDatumComparator querySpaceComparator;

    /**
     * The Comparator used to do compare in the partition IntFunc search space
     */
    protected SearchDatumComparator pruningSpaceComparator;

    /**
     * The Comparator used to do compare in the partition bound value space
     */
    protected SearchDatumComparator boundSpaceComparator;

    /**
     * The Hasher used to do hash and convert search
     */
    protected SearchDatumHasher hasher;

    /**
     * The router for partition pruning
     */
    protected PartitionRouter router;

    /**
     * Label if need enum range
     */
    protected boolean needEnumRange;

    /**
     * The sql operator of the The partition function
     */
    protected SqlOperator partIntFuncOperator;

    /**
     * The partition function for hash/range/list partitions
     */
    protected PartitionIntFunction partIntFunc;

    /**
     * The monotonicity of part int func
     */
    protected Monotonicity partIntFuncMonotonicity;

    public PartitionByDefinition() {
        partitions = new ArrayList<>();
        partitionExprList = new ArrayList<>();
        partitionExprTypeList = new ArrayList<>();
        partitionFieldList = new ArrayList<>();
        partitionColumnNameList = new ArrayList<>();
    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
    }

    public List<PartitionSpec> getPartitions() {
        return partitions;
    }

    /**
     * nth starts from 1
     */
    public PartitionSpec getNthPartition(int nth) {
        return this.partitions.get(nth - 1);
    }

    public PartitionSpec getPartitionByPartName(String partName) {
        for (int i = 0; i < partitions.size(); i++) {
            String name = partitions.get(i).getName();
            if (partName.equalsIgnoreCase(name)) {
                return partitions.get(i);
            }
        }
        return null;
    }

    public List<String> getActualPartitionColumns() {
        List<String> fullPartColList = this.partitionColumnNameList;
        int fullPartColCnt = fullPartColList.size();
        int actualPartColCnt = -1;
        List<String> targetPartColList = new ArrayList<>();
        if (strategy == PartitionStrategy.HASH) {
            actualPartColCnt = fullPartColCnt;
        } else if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            actualPartColCnt = fullPartColCnt;
        } else {
            boolean findActualPartColCnt = false;
            for (int i = fullPartColCnt - 1; i > -1; i--) {
                for (int j = 0; j < this.partitions.size(); j++) {
                    PartitionSpec spec = this.partitions.get(j);
                    SingleValuePartitionBoundSpec bndSpec = (SingleValuePartitionBoundSpec) spec.getBoundSpec();
                    PartitionBoundVal[] bndVal = bndSpec.getSingleDatum().getDatumInfo();
                    PartitionBoundVal bndValOfOneCol = bndVal[i];
                    PartitionBoundValueKind bndValKind = bndVal[i].getValueKind();

                    if (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.HASH) {
                        if (bndValKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                            actualPartColCnt = i+1;
                            findActualPartColCnt = true;
                            break;
                        }
                        PartitionField valFld = bndValOfOneCol.getValue();
                        if (valFld.longValue() != Long.MAX_VALUE) {
                            actualPartColCnt = i+1;
                            findActualPartColCnt = true;
                            break;
                        }

                    } else if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS) {
                        if (bndValKind != PartitionBoundValueKind.DATUM_MAX_VALUE) {
                            actualPartColCnt = i+1;
                            findActualPartColCnt = true;
                            break;
                        }
                    } else {
                        throw new NotSupportException("Not support for list/list columns partitions");
                    }
                    continue;
                }
                if (findActualPartColCnt) {
                    break;
                }
            }
        }
        /**
         * actualPartColCnt must be >= 1
         */
        if (actualPartColCnt == 0 && fullPartColCnt > 0) {
            actualPartColCnt = 1;
        }
        for (int i = 0; i < actualPartColCnt; i++) {
            targetPartColList.add(fullPartColList.get(i));
        }
        return targetPartColList;
    }

    public void setPartitions(List<PartitionSpec> partitions) {
        this.partitions = partitions;
    }

    public List<SqlNode> getPartitionExprList() {
        return partitionExprList;
    }

    public void setPartitionExprList(List<SqlNode> partitionExprList) {
        this.partitionExprList = partitionExprList;
    }

    public List<ColumnMeta> getPartitionFieldList() {
        return partitionFieldList;
    }

    public void setPartitionFieldList(List<ColumnMeta> partitionFieldList) {
        this.partitionFieldList = partitionFieldList;
    }

    public List<String> getPartitionColumnNameList() {
        return partitionColumnNameList;
    }

    public void setPartitionColumnNameList(List<String> partitionColumnNameList) {
        for (int i = 0; i < partitionColumnNameList.size(); i++) {
            partitionColumnNameList.set(i, partitionColumnNameList.get(i).toLowerCase());
        }
        this.partitionColumnNameList = partitionColumnNameList;
    }

    public String normalizePartitionByInfo(TableGroupConfig tableGroupConfig, boolean showHashByRange, int prefixPartColCnt) {

        Map<Long, String> partGrpNameInfo = new HashMap<>();
        tableGroupConfig.getPartitionGroupRecords().stream().forEach(o -> partGrpNameInfo.put(o.id, o.partition_name));
        return normalizePartitionByInfoInner(true, partGrpNameInfo, true, showHashByRange, prefixPartColCnt);
    }

    public String normalizePartitionKeyIgnoreColName(int prefixPartCol) {

        StringBuilder sb = new StringBuilder();
        sb.append(strategy.toString());
        sb.append("(");
        int i = 0;

        int fullPartCount = getPartitionColumnNameList().size();
        int partCount = fullPartCount;
        if (prefixPartCol != PartitionInfoUtil.FULL_PART_COL_COUNT) {
            if (prefixPartCol > fullPartCount) {
                partCount = fullPartCount;
            } else {
                partCount = prefixPartCol;
            }
        }
        for (int j = 0; j < partCount; j++) {
            SqlNode sqlNode = partitionExprList.get(j);
            if (i > 0) {
                sb.append(",");
            }
            ColumnMeta columnMeta = partitionFieldList.get(i++);
            if (sqlNode instanceof SqlCall) {
                sb.append(((SqlCall) sqlNode).getOperator().getName());
                sb.append("(");
                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
                sb.append(")");
            } else {
                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
            }
        }
        sb.append(")");
        return sb.toString();
    }


    private String normalizePartitionByInfoInner(boolean usePartGroupNameAsPartName,
                                                 Map<Long, String> partGrpNameInfo,
                                                 boolean needSortPartitions,
                                                 boolean showHashByRange,
                                                 int prefixPartColCnt) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY ");
        sb.append(strategy.toString());
        sb.append("(");
        int i = 0;
        assert partitionFieldList.size() == partitionExprList.size();
        int targetPartColCnt = partitionFieldList.size();
        if (prefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT) {
            targetPartColCnt = prefixPartColCnt;
        }
        for (int j = 0; j < targetPartColCnt; j++) {
            SqlNode sqlNode  = partitionExprList.get(j);
            if (i > 0) {
                sb.append(",");
            }
            ColumnMeta columnMeta = partitionFieldList.get(i++);
            if (sqlNode instanceof SqlCall) {
                sb.append(((SqlCall) sqlNode).getOperator().getName());
                sb.append("(");
                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
                sb.append(")");
            } else {
                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
            }
        }
        sb.append(")\n");
        normalizePartitions(sb, usePartGroupNameAsPartName, partGrpNameInfo, needSortPartitions, showHashByRange, prefixPartColCnt);
        return sb.toString();
    }

    public String normalizePartitionByDefForShowCreateTable(boolean showHashByRange) {

        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY ");
        sb.append(strategy.getStrategyExplainName().toUpperCase());
        sb.append("(");
        int i = 0;
        assert partitionFieldList.size() == partitionExprList.size();
        for (SqlNode sqlNode : partitionExprList) {
            if (i > 0) {
                sb.append(",");
            }
            ColumnMeta columnMeta = partitionFieldList.get(i++);
            if (sqlNode instanceof SqlCall) {
                sb.append(((SqlCall) sqlNode).getOperator().getName());
                sb.append("(");
                sb.append(SqlIdentifier.surroundWithBacktick(columnMeta.getName()));
                sb.append(")");
            } else {
                sb.append(SqlIdentifier.surroundWithBacktick((columnMeta.getName())));
            }

        }
        sb.append(")\n");
        normalizePartitions(sb, false, null, false, showHashByRange, PartitionInfoUtil.FULL_PART_COL_COUNT);
        return sb.toString();
    }

    /**
     * @param sb output string builder
     * @param partGrpNameInfo partGrpNameInfo
     * @param needSortPartitions label if need sort the bound of partitions
     * @param showHashByRange use range format to show hash/key partitions
     */
    private void normalizePartitions(StringBuilder sb,
                                     boolean usePartGroupNameAsPartName,
                                     Map<Long, String> partGrpNameInfo,
                                     boolean needSortPartitions,
                                     boolean showHashByRange,
                                     int prefixPartColCnt) {
        int i;
        switch (strategy) {
        case KEY:
        case HASH:
            sb.append("PARTITIONS ");
            sb.append(partitions.size());
            if (!showHashByRange) {
                break;
            } else {
                sb.append("\n");
            }
        case LIST:
        case LIST_COLUMNS:
        case RANGE:
        case RANGE_COLUMNS:
            sb.append("(");
            i = 0;
            List<PartitionSpec> partSpecList = partitions;
            if (needSortPartitions) {
                partSpecList = getOrderedPartitionSpec();
            }
            for (PartitionSpec pSpec : partSpecList) {
                if (i > 0) {
                    sb.append(",\n ");
                }
                String partGrpName = null;
                if (usePartGroupNameAsPartName && partGrpNameInfo != null) {
                    partGrpName = partGrpNameInfo.get(pSpec.getLocation().getPartitionGroupId());
                }
                sb.append(pSpec.normalizePartSpec(usePartGroupNameAsPartName, partGrpName, false, prefixPartColCnt));
                i++;
            }
            sb.append(")");
            break;
        }
        //sb.append('\n');
    }

    @Override
    public String toString() {
        return normalizePartitionByDefForShowCreateTable(false);
    }

    public PartitionByDefinition copy() {

        PartitionByDefinition newPartDef = new PartitionByDefinition();

        newPartDef.setStrategy(this.strategy);
        List<PartitionSpec> newPartitions = new ArrayList<>();
        for (int i = 0; i < this.getPartitions().size(); i++) {
            newPartitions.add(this.getPartitions().get(i).copy());
        }
        newPartDef.setPartitions(newPartitions);

        List<SqlNode> newPartitionExprList = new ArrayList<>();
        newPartitionExprList.addAll(this.getPartitionExprList());
        newPartDef.setPartitionExprList(newPartitionExprList);

        List<RelDataType> newPartitionExprTypeList = new ArrayList<>();
        newPartitionExprTypeList.addAll(this.getPartitionExprTypeList());
        newPartDef.setPartitionExprTypeList(newPartitionExprTypeList);

        List<ColumnMeta> newPartitionFieldList = new ArrayList<>();
        newPartitionFieldList.addAll(this.getPartitionFieldList());
        newPartDef.setPartitionFieldList(newPartitionFieldList);

        List<String> newPartitionColumnNameList = new ArrayList<>();
        newPartitionColumnNameList.addAll(this.getPartitionColumnNameList());
        newPartDef.setPartitionColumnNameList(newPartitionColumnNameList);

        newPartDef.setNeedEnumRange(this.isNeedEnumRange());

        newPartDef.setPruningSpaceComparator(this.getPruningSpaceComparator());
        newPartDef.setHasher(this.getHasher());
        newPartDef.setBoundSpaceComparator(this.getBoundSpaceComparator());
        newPartDef.setPartIntFuncOperator(this.getPartIntFuncOperator());
        newPartDef.setPartIntFunc(this.getPartIntFunc());
        newPartDef.setPartIntFuncMonotonicity(this.getPartIntFuncMonotonicity());
        newPartDef.setQuerySpaceComparator(this.getQuerySpaceComparator());
        newPartDef.setRouter(this.getRouter());

        return newPartDef;
    }

    protected static SearchDatumComparator getBoundSpaceComparator(SearchDatumComparator pruningSpaceComparator,
                                                                   PartitionStrategy strategy) {

        int partCnt = pruningSpaceComparator.getDatumRelDataTypes().length;
        RelDataTypeFactory typeFactory = PartitionPrunerUtils.getTypeFactory();
        RelDataType[] datumDataTypes = new RelDataType[partCnt];
        DataType[] datumDrdsDataTypes = new DataType[partCnt];
        Charset[] datumCharsets = new Charset[partCnt];
        SqlCollation[] datumCollations = new SqlCollation[partCnt];
        for (int i = 0; i < partCnt; i++) {
            RelDataType partExprDataType = pruningSpaceComparator.getDatumRelDataTypes()[i];
            RelDataType partBndValDataType =
                PartitionInfoBuilder.getDataTypeForBoundVal(typeFactory, strategy, partExprDataType);
            datumDataTypes[i] = partBndValDataType;
            datumDrdsDataTypes[i] = DataTypeUtil.calciteToDrdsType(datumDataTypes[i]);
            datumCharsets[i] = datumDataTypes[i].getCharset();
            datumCollations[i] = datumDataTypes[i].getCollation();
        }
        SearchDatumComparator cmp =
            new SearchDatumComparator(datumDataTypes, datumDrdsDataTypes, datumCharsets, datumCollations);
        return cmp;
    }

    protected static SearchDatumComparator getPruningSpaceComparator(List<RelDataType> partitionExprTypeList) {
        if (partitionExprTypeList.isEmpty()) {
            return null;
        }
        int partCnt = partitionExprTypeList.size();
        RelDataType[] datumDataTypes = new RelDataType[partCnt];
        DataType[] datumDrdsDataTypes = new DataType[partCnt];
        Charset[] datumCharsets = new Charset[partCnt];
        SqlCollation[] datumCollations = new SqlCollation[partCnt];
        for (int i = 0; i < partCnt; i++) {
            RelDataType partExprDataType = partitionExprTypeList.get(i);
            datumDataTypes[i] = partExprDataType;
            datumDrdsDataTypes[i] = DataTypeUtil.calciteToDrdsType(datumDataTypes[i]);
            datumCharsets[i] = datumDataTypes[i].getCharset();
            datumCollations[i] = datumDataTypes[i].getCollation();
        }
        SearchDatumComparator cmp =
            new SearchDatumComparator(datumDataTypes, datumDrdsDataTypes, datumCharsets, datumCollations);
        return cmp;
    }

    protected static SearchDatumComparator getQuerySpaceComparator(List<ColumnMeta> partitionFieldList) {
        if (partitionFieldList.isEmpty()) {
            return null;
        }
        int partCnt = partitionFieldList.size();
        RelDataType[] datumDataTypes = new RelDataType[partCnt];
        DataType[] datumDrdsDataTypes = new DataType[partCnt];
        Charset[] datumCharsets = new Charset[partCnt];
        SqlCollation[] datumCollations = new SqlCollation[partCnt];
        for (int i = 0; i < partCnt; i++) {
            RelDataType partExprDataType = partitionFieldList.get(i).getField().getRelType();
            datumDataTypes[i] = partExprDataType;
            datumDrdsDataTypes[i] = DataTypeUtil.calciteToDrdsType(datumDataTypes[i]);
            datumCharsets[i] = datumDataTypes[i].getCharset();
            datumCollations[i] = datumDataTypes[i].getCollation();
        }
        SearchDatumComparator cmp =
            new SearchDatumComparator(datumDataTypes, datumDrdsDataTypes, datumCharsets, datumCollations);
        return cmp;
    }

    protected static SearchDatumHasher getHasher(PartitionStrategy strategy,
                                                 List<ColumnMeta> partitionFieldList,
                                                 List<RelDataType> partitionExprTypeList) {
        if (partitionFieldList.isEmpty()) {
            return null;
        }
        int partCnt = partitionFieldList.size();
        RelDataType[] datumDataTypes = new RelDataType[partCnt];
        DataType[] datumDrdsDataTypes = new DataType[partCnt];
        Charset[] datumCharsets = new Charset[partCnt];
        SqlCollation[] datumCollations = new SqlCollation[partCnt];
        for (int i = 0; i < partCnt; i++) {
            RelDataType partExprDataType = partitionExprTypeList.get(i);
            datumDataTypes[i] = partExprDataType;
            datumDrdsDataTypes[i] = DataTypeUtil.calciteToDrdsType(datumDataTypes[i]);
            datumCharsets[i] = datumDataTypes[i].getCharset();
            datumCollations[i] = datumDataTypes[i].getCollation();
        }
        boolean isKeyPart = strategy == PartitionStrategy.KEY;
        SearchDatumHasher hasher =
            new SearchDatumHasher(isKeyPart, datumDataTypes, datumDrdsDataTypes, datumCharsets, datumCollations);
        return hasher;
    }

    private static Long extractHashCodeFromPartitionBound(PartitionBoundSpec boundSpec) {
        return boundSpec.getSingleDatum().getSingletonValue().getValue().longValue();
    }

    protected static PartitionRouter getPartRouter(PartitionByDefinition partitionBy) {

        PartitionRouter router = null;
        Comparator comp = partitionBy.getPruningSpaceComparator();
        Comparator bndComp = partitionBy.getBoundSpaceComparator();
        PartitionByDefinition partDef = partitionBy;
        PartitionStrategy strategy = partDef.getStrategy();
        SearchDatumHasher hasher = partitionBy.getHasher();

        if (strategy.isHashed()) {
            // Extract hash value for hash-partition

            if (!strategy.isKey() || (strategy.isKey() && partitionBy.getPartitionFieldList().size() == 1)) {
                Object[] datumArr = partDef.getPartitions().stream()
                    .map(part -> extractHashCodeFromPartitionBound(part.getBoundSpec())).toArray();
                router = PartitionRouter.createByHasher(strategy, datumArr, hasher, null/*use Long Comp*/);
            } else {
                Object[] datumArr = partDef.getPartitions().stream()
                    .map(part -> part.getBoundSpec().getSingleDatum()).toArray();
                router = PartitionRouter.createByHasher(strategy, datumArr, hasher, bndComp);
            }

        } else if (strategy.isRange()) {
            Object[] datumArr = partDef.getPartitions().stream()
                .map(part -> part.getBoundSpec().getSingleDatum()).toArray();
            router = PartitionRouter.createByComparator(strategy, datumArr, comp);
        } else if (strategy.isList()) {
            TreeMap<Object, Integer> boundValPartPosiInfo = new TreeMap<>(comp);
            for (PartitionSpec ps : partDef.getPartitions()) {
                for (SearchDatumInfo datum : ps.getBoundSpec().getMultiDatums()) {
                    boundValPartPosiInfo.put(datum, ps.getPosition().intValue());
                }
            }
            router = PartitionRouter.createByList(strategy, boundValPartPosiInfo, comp);
        } else {
            throw new UnsupportedOperationException("partition strategy " + strategy);
        }

        return router;
    }

    public List<PartitionSpec> getOrderedPartitionSpec() {
        if (this.strategy != PartitionStrategy.LIST && this.strategy != PartitionStrategy.LIST_COLUMNS) {
            return this.partitions;
        }
        List<PartitionSpec> newPartSpecList = new ArrayList<>();
        SearchDatumComparator cmp = getBoundSpaceComparator();
        TreeMap<SearchDatumInfo, PartitionSpec> partFirstValMap = new TreeMap(cmp);
        TreeMap<SearchDatumInfo, PartitionSpec> listValPartMap = new TreeMap(cmp);
        for (int i = 0; i < this.partitions.size(); i++) {
            PartitionSpec pSpec = this.partitions.get(i);
            PartitionSpec newSpec = pSpec.copy();
            PartitionBoundSpec newSortedValsBndSpec = sortListPartitionsAllValues(cmp, newSpec.getBoundSpec());

            for (int j = 0; j < newSortedValsBndSpec.getMultiDatums().size(); j++) {
                SearchDatumInfo oneListVal = newSortedValsBndSpec.getMultiDatums().get(j);
                if (listValPartMap.containsKey(oneListVal)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "Multiple definition of same constant in list partitioning");
                }
                listValPartMap.put(oneListVal, newSpec);
            }

            newSpec.setBoundSpec(newSortedValsBndSpec);
            SearchDatumInfo firstValOfOnePart = newSortedValsBndSpec.getMultiDatums().get(0);
            partFirstValMap.put(firstValOfOnePart, newSpec);
        }

        for (SearchDatumInfo datumInfo : partFirstValMap.keySet()) {
            newPartSpecList.add(partFirstValMap.get(datumInfo));
        }
        return newPartSpecList;
    }

    protected static PartitionBoundSpec sortListPartitionsAllValues(
        SearchDatumComparator cmp,
        PartitionBoundSpec bndSpec) {
        if (bndSpec.isSingleValue()) {
            throw new RuntimeException("should be multi-value bound");
        }

        PartitionBoundSpec newBndSpec = bndSpec.copy();
        newBndSpec.getMultiDatums().sort(cmp);
        return newBndSpec;
    }

    @Override
    public int hashCode() {

        int hashCodeVal = strategy.hashCode();
        int partColCnt = partitionExprList.size();
        for (int i = 0; i < partColCnt; i++) {
            String partColName = partitionColumnNameList.get(i);
            hashCodeVal ^= partColName.toLowerCase().hashCode();

            ColumnMeta partColMeta = partitionFieldList.get(i);
            hashCodeVal ^= partColMeta.hashCode();

            SqlNode partColExprAst = partitionExprList.get(i);
            hashCodeVal ^= partColExprAst.toString().hashCode();
        }

        int partCnt = partitions.size();
        for (int i = 0; i < partCnt; i++) {
            PartitionSpec pSpec = partitions.get(i);
            hashCodeVal ^= pSpec.hashCode();
        }

        return hashCodeVal;
    }

//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj) {
//            return true;
//        }
//        if (obj != null && obj.getClass() == this.getClass()) {
//            if (strategy != ((PartitionByDefinition) (obj)).getStrategy()) {
//                return false;
//            }
//            int partColCnt = partitionExprList.size();
//            PartitionByDefinition other = (PartitionByDefinition) obj;
//            if (other.getPartitionExprList().size() != partColCnt) {
//                return false;
//            }
//            for (int i = 0; i < partColCnt; i++) {
//
//                ColumnMeta partColMeta = partitionFieldList.get(i);
//                ColumnMeta otherPartColMeta = other.getPartitionFieldList().get(i);
//                CharsetName charsetName = partColMeta.getField().getDataType().getCharsetName();
//                CharsetName otherCharsetName = otherPartColMeta.getField().getDataType().getCharsetName();
//
//                boolean isCharsetDiff = (charsetName == null && otherCharsetName != null)
//                    || (charsetName != null && otherCharsetName == null)
//                    || (charsetName != null && otherCharsetName != null && !charsetName.equals(otherCharsetName));
//                if (isCharsetDiff) {
//                    return false;
//                }
//
//                CollationName collationName = partColMeta.getField().getDataType().getCollationName();
//                CollationName otherCollationName = otherPartColMeta.getField().getDataType().getCollationName();
//
//                boolean isCollationDiff = (collationName == null && otherCollationName != null)
//                    || (collationName != null && otherCollationName == null)
//                    || (collationName != null && otherCollationName != null && !collationName
//                    .equals(otherCollationName));
//                if (isCollationDiff) {
//                    return false;
//                }
//
//                if (partColMeta.getDataType() == null) {
//                    if (otherPartColMeta.getDataType() != null) {
//                        return false;
//                    }
//                } else if (!DataTypeUtil
//                    .equals(partColMeta.getDataType(), otherPartColMeta.getDataType(), true)) {
//                    return false;
//                }
//
//                if (partIntFunc == null && other.getPartIntFunc() != null ||
//                    partIntFunc != null && other.getPartIntFunc() == null) {
//                    return false;
//                } else if (partIntFunc != null) {
//                    if (partIntFunc.getFunctionNames().length != other.getPartIntFunc().getFunctionNames().length) {
//                        return false;
//                    } else {
//                        for (int j = 0; j < partIntFunc.getFunctionNames().length; j++) {
//                            if (partIntFunc.getFunctionNames()[j] == null
//                                && other.getPartIntFunc().getFunctionNames()[j] != null ||
//                                partIntFunc.getFunctionNames()[j] != null
//                                    && other.getPartIntFunc().getFunctionNames()[j] == null) {
//                                return false;
//                            }
//                            if (!partIntFunc.getFunctionNames()[j]
//                                .equalsIgnoreCase(other.getPartIntFunc().getFunctionNames()[j])) {
//                                return false;
//                            }
//                        }
//                    }
//                }
//            }
//            List<PartitionSpec> partitionSpecs1 = getOrderedPartitionSpec();
//            List<PartitionSpec> partitionSpecs2 = ((PartitionByDefinition) (obj)).getOrderedPartitionSpec();
//            if (GeneralUtil.isNotEmpty(partitionSpecs1) && GeneralUtil.isNotEmpty(partitionSpecs2)
//                && partitionSpecs1.size() == partitionSpecs2.size()) {
//                for (int i = 0; i < partitionSpecs1.size(); i++) {
//                    if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i).equals(partitionSpecs2.get(i))) {
//                        return false;
//                    }
//                }
//                return true;
//            }
//        }
//        return false;
//    }

    @Override
    public boolean equals(Object obj) {
        return equals(obj, -1);
    }

    public boolean equals(Object obj, int prefixPartColCnt) {
        if (this == obj) {
            return true;
        }
        if (obj != null && obj.getClass() == this.getClass()) {
            if (strategy != ((PartitionByDefinition) (obj)).getStrategy()) {
                return false;
            }
            PartitionByDefinition other = (PartitionByDefinition) obj;
            int partColCnt = partitionExprList.size();
            int partColCntOther = other.getPartitionColumnNameList().size();

            int actualPartColCnt = partColCnt;
            int actualPartColCntOther = partColCntOther;
            if (prefixPartColCnt > 0) {
                actualPartColCnt = getActualPartitionColumns().size();
                actualPartColCntOther = other.getActualPartitionColumns().size();
            }

            int comparePartColCnt = partColCnt;
            if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                if (other.getPartitionExprList().size() != partColCnt) {
                    return false;
                }
            } else {

                if (actualPartColCnt != actualPartColCntOther) {
                    return false;
                }

                int minPartColCnt = partColCnt > partColCntOther ? partColCntOther : partColCnt;
                if (minPartColCnt <= prefixPartColCnt) {
                    comparePartColCnt = minPartColCnt;
                } else {
                    if (prefixPartColCnt < actualPartColCnt) {
                        comparePartColCnt = actualPartColCnt;
                    } else {
                        comparePartColCnt = prefixPartColCnt;
                    }
                }
            }

            for (int i = 0; i < comparePartColCnt; i++) {
                if (!PartitionInfoUtil.partitionDataTypeEquals(partitionFieldList.get(i), other.getPartitionFieldList().get(i))) {
                    return false;
                }

                if (partIntFunc == null && other.getPartIntFunc() != null ||
                    partIntFunc != null && other.getPartIntFunc() == null) {
                    return false;
                } else if (partIntFunc != null) {
                    if (partIntFunc.getFunctionNames().length != other.getPartIntFunc().getFunctionNames().length) {
                        return false;
                    } else {
                        for (int j = 0; j < partIntFunc.getFunctionNames().length; j++) {
                            if (partIntFunc.getFunctionNames()[j] == null
                                && other.getPartIntFunc().getFunctionNames()[j] != null ||
                                partIntFunc.getFunctionNames()[j] != null
                                    && other.getPartIntFunc().getFunctionNames()[j] == null) {
                                return false;
                            }
                            if (!partIntFunc.getFunctionNames()[j]
                                .equalsIgnoreCase(other.getPartIntFunc().getFunctionNames()[j])) {
                                return false;
                            }
                        }
                    }
                }
            }
            List<PartitionSpec> partitionSpecs1 = getOrderedPartitionSpec();
            List<PartitionSpec> partitionSpecs2 = ((PartitionByDefinition) (obj)).getOrderedPartitionSpec();
            if (GeneralUtil.isNotEmpty(partitionSpecs1) && GeneralUtil.isNotEmpty(partitionSpecs2) && partitionSpecs1.size() == partitionSpecs2.size()) {
                if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT ) {
                    for (int i = 0; i < partitionSpecs1.size(); i++) {
                        if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i).equals(partitionSpecs2.get(i))) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    for (int i = 0; i < partitionSpecs1.size(); i++) {
                        if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i).equals(partitionSpecs2.get(i), comparePartColCnt)) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Types of partition columns
     */
    public List<DataType> getPartitionColumnTypeList() {
        return this.partitionFieldList.stream().map(ColumnMeta::getDataType).collect(Collectors.toList());
    }

    /**
     * Types of partition expression
     */
    public List<RelDataType> getPartitionExprTypeList() {
        return partitionExprTypeList;
    }

    public void setPartitionExprTypeList(List<RelDataType> partitionExprTypeList) {
        this.partitionExprTypeList = partitionExprTypeList;
    }

    public SearchDatumComparator getPruningSpaceComparator() {
        return this.pruningSpaceComparator;
    }

    public void setPruningSpaceComparator(SearchDatumComparator comparator) {
        this.pruningSpaceComparator = comparator;
    }

    public void setHasher(SearchDatumHasher hasher) {
        this.hasher = hasher;
    }

    public SearchDatumHasher getHasher() {
        return hasher;
    }

    public SearchDatumComparator getQuerySpaceComparator() {
        return querySpaceComparator;
    }

    public void setQuerySpaceComparator(SearchDatumComparator querySpaceComparator) {
        this.querySpaceComparator = querySpaceComparator;
    }

    public boolean isNeedEnumRange() {
        return needEnumRange;
    }

    public void setNeedEnumRange(boolean needEnumRange) {
        this.needEnumRange = needEnumRange;
    }

    public PartitionIntFunction getPartIntFunc() {
        return partIntFunc;
    }

    public Monotonicity getPartIntFuncMonotonicity() {
        return partIntFuncMonotonicity;
    }

    public void setPartIntFunc(PartitionIntFunction partIntFunc) {
        this.partIntFunc = partIntFunc;
    }

    public void setPartIntFuncMonotonicity(Monotonicity partIntFuncMonotonicity) {
        this.partIntFuncMonotonicity = partIntFuncMonotonicity;
    }

    public SqlOperator getPartIntFuncOperator() {
        return partIntFuncOperator;
    }

    public void setPartIntFuncOperator(SqlOperator partIntFuncOperator) {
        this.partIntFuncOperator = partIntFuncOperator;
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public MySQLIntervalType getIntervalType() {
        if (partIntFunc != null) {
            return partIntFunc.getIntervalType();
        }
        return null;
    }

    public PartitionRouter getRouter() {
        return router;
    }

    protected void setRouter(PartitionRouter router) {
        this.router = router;
    }
}
