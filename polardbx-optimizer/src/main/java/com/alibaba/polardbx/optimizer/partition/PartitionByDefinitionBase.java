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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.ListPartRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import groovy.sql.Sql;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public abstract class PartitionByDefinitionBase {

    /**
     * the partition strategy of current partitions
     */
    protected PartitionStrategy strategy;

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

    /**
     * The array of part partition function for all partition columns,
     * <pre>
     *      some strategy like co_hash, allow using multi partFunc-wrapped partition columns,
     *      so the size of this array may be >= 1;
     *      but some strategy like hash/range/list/udf_hash, they allow using only one
     *      partFunc-wrapped partition column, so the size of this array will be  <= 1
     *  </pre>
     */
    protected PartitionIntFunction[] partFuncArr;

    public PartitionByDefinitionBase() {
        partitionExprList = new ArrayList<>();
        partitionExprTypeList = new ArrayList<>();
        partitionFieldList = new ArrayList<>();
        partitionColumnNameList = new ArrayList<>();
        partFuncArr = null;
    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
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

    public List<RelDataType> getPartitionExprTypeList() {
        return partitionExprTypeList;
    }

    public void setPartitionExprTypeList(List<RelDataType> partitionExprTypeList) {
        this.partitionExprTypeList = partitionExprTypeList;
    }

    public SearchDatumComparator getQuerySpaceComparator() {
        return querySpaceComparator;
    }

    public void setQuerySpaceComparator(
        SearchDatumComparator querySpaceComparator) {
        this.querySpaceComparator = querySpaceComparator;
    }

    public SearchDatumComparator getPruningSpaceComparator() {
        return pruningSpaceComparator;
    }

    public void setPruningSpaceComparator(
        SearchDatumComparator pruningSpaceComparator) {
        this.pruningSpaceComparator = pruningSpaceComparator;
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(
        SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public SearchDatumHasher getHasher() {
        return hasher;
    }

    public void setHasher(SearchDatumHasher hasher) {
        this.hasher = hasher;
    }

    public PartitionRouter getRouter() {
        return router;
    }

    public void setRouter(PartitionRouter router) {
        this.router = router;
    }

    public boolean isNeedEnumRange() {
        return needEnumRange;
    }

    public void setNeedEnumRange(boolean needEnumRange) {
        this.needEnumRange = needEnumRange;
    }

    public SqlOperator getPartIntFuncOperator() {
        return partIntFuncOperator;
    }

    public void setPartIntFuncOperator(SqlOperator partIntFuncOperator) {
        this.partIntFuncOperator = partIntFuncOperator;
    }

    public PartitionIntFunction getPartIntFunc() {
        return partIntFunc;
    }

    public void setPartIntFunc(PartitionIntFunction partIntFunc) {
        this.partIntFunc = partIntFunc;
    }

    public Monotonicity getPartIntFuncMonotonicity() {
        return partIntFuncMonotonicity;
    }

    public void setPartIntFuncMonotonicity(
        Monotonicity partIntFuncMonotonicity) {
        this.partIntFuncMonotonicity = partIntFuncMonotonicity;
    }

    protected static SearchDatumComparator buildBoundSpaceComparator(SearchDatumComparator pruningSpaceComparator,
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

    protected static SearchDatumComparator buildPruningSpaceComparator(List<RelDataType> partitionExprTypeList) {
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

    protected static SearchDatumComparator buildQuerySpaceComparator(List<ColumnMeta> partitionFieldList) {
        if (partitionFieldList.isEmpty()) {
            return null;
        }
        int partCnt = partitionFieldList.size();
        RelDataType[] datumDataTypes = new RelDataType[partCnt];
        DataType[] datumDrdsDataTypes = new DataType[partCnt];
        Charset[] datumCharsets = new Charset[partCnt];
        SqlCollation[] datumCollations = new SqlCollation[partCnt];
        for (int i = 0; i < partCnt; i++) {
            Field partFld = partitionFieldList.get(i).getField();
            RelDataType partFldDataType = partFld.getRelType();
            datumDataTypes[i] = partFldDataType;
            datumDrdsDataTypes[i] = DataTypeUtil.calciteToDrdsType(datumDataTypes[i]);
            datumCharsets[i] = datumDataTypes[i].getCharset();
            datumCollations[i] = datumDataTypes[i].getCollation();
        }
        SearchDatumComparator cmp =
            new SearchDatumComparator(datumDataTypes, datumDrdsDataTypes, datumCharsets, datumCollations);
        return cmp;
    }

    public static SearchDatumComparator buildQuerySpaceComparatorBySpecifyDataTypes(
        List<RelDataType> partFldRelDataTypes,
        List<DataType> partFldDataTypes
    ) {
        if (partFldRelDataTypes.isEmpty()) {
            return null;
        }
        int partCnt = partFldRelDataTypes.size();
        RelDataType[] datumDataTypes = new RelDataType[partCnt];
        DataType[] datumDrdsDataTypes = new DataType[partCnt];
        Charset[] datumCharsets = new Charset[partCnt];
        SqlCollation[] datumCollations = new SqlCollation[partCnt];
        for (int i = 0; i < partCnt; i++) {
            RelDataType partFldRelDataType = partFldRelDataTypes.get(i);
            DataType partFldDataType = partFldDataTypes.get(i);
            datumDataTypes[i] = partFldRelDataType;
            datumDrdsDataTypes[i] = partFldDataType;
            datumCharsets[i] = datumDataTypes[i].getCharset();
            datumCollations[i] = datumDataTypes[i].getCollation();
        }
        SearchDatumComparator cmp =
            new SearchDatumComparator(datumDataTypes, datumDrdsDataTypes, datumCharsets, datumCollations);
        return cmp;
    }

    protected static SearchDatumHasher buildHasher(PartitionStrategy strategy,
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
        PartitionBoundVal bndVal = boundSpec.getSingleDatum().getSingletonValue();
        if (bndVal.isMaxValue()) {
            return PartitionBoundVal.HASH_BOUND_MAX_VAL_LONG;
        } else if (bndVal.isMinValue()) {
            return PartitionBoundVal.HASH_BOUND_MIN_VAL_LONG;
        } else {
            return bndVal.getValue().longValue();
        }
    }

    protected static PartitionRouter buildPartRouterInner(SearchDatumComparator partPruningSpaceComp,
                                                          SearchDatumComparator partBoundSpaceCmp,
                                                          SearchDatumHasher partHasher,
                                                          PartitionStrategy partStrategy,
                                                          List<ColumnMeta> partFldList,
                                                          PartitionIntFunction[] partFuncArr,
                                                          List<PartitionSpec> partitions) {

        PartitionRouter router = null;
        Comparator comp = partPruningSpaceComp;
        Comparator bndComp = partBoundSpaceCmp;
        PartitionStrategy strategy = partStrategy;
        SearchDatumHasher hasher = partHasher;
        List<ColumnMeta> partFields = partFldList;

        if (partitions.isEmpty()) {
            return null;
        }

        if (strategy.isDirectHash()) {
            router = PartitionRouter.createByDirectHasher(partitions.size());
        } else if (strategy.isHashed()) {
            // Extract hash value for hash-partition

            if (!strategy.isKey() || (strategy.isKey() && partFields.size() == 1)) {
                Object[] datumArr = partitions.stream()
                    .map(part -> extractHashCodeFromPartitionBound(part.getBoundSpec())).toArray();
                router = PartitionRouter.createByHasher(strategy, datumArr, hasher, null/*use Long Comp*/);
            } else {
                Object[] datumArr = partitions.stream()
                    .map(part -> part.getBoundSpec().getSingleDatum()).toArray();
                router = PartitionRouter.createByHasher(strategy, datumArr, hasher, bndComp);
            }

        } else if (strategy.isRange()) {
            Object[] datumArr = partitions.stream()
                .map(part -> part.getBoundSpec().getSingleDatum()).toArray();
            router = PartitionRouter.createByComparator(strategy, datumArr, comp);
        } else if (strategy.isList()) {
            TreeMap<Object, Integer> boundValPartPosiInfo = new TreeMap<>(comp);
            boolean containDefaultPartition = false;
            int defaultPartitionPosition = 0;
            for (PartitionSpec ps : partitions) {
                if (ps.isDefaultPartition()) {
                    containDefaultPartition = true;
                    defaultPartitionPosition = ps.getPosition().intValue();
                    continue;
                }
                for (SearchDatumInfo datum : ps.getBoundSpec().getMultiDatums()) {
                    boundValPartPosiInfo.put(datum, ps.getPosition().intValue());
                }
            }
            router = PartitionRouter.createByList(strategy, boundValPartPosiInfo, comp, containDefaultPartition);
            if (containDefaultPartition && router instanceof ListPartRouter) {
                ((ListPartRouter) router).setHasDefaultPartition(true);
                ((ListPartRouter) router).setDefaultPartitionPosition(defaultPartitionPosition);
            }
        } else if (strategy.isUdfHashed()) {
            Object[] datumArr = partitions.stream()
                .map(part -> extractHashCodeFromPartitionBound(part.getBoundSpec())).toArray();
            router = PartitionRouter.createByHasher(strategy, datumArr, hasher, null/*use Long Comp*/);

        } else if (strategy.isCoHashed()) {
            Object[] datumArr = partitions.stream()
                .map(part -> extractHashCodeFromPartitionBound(part.getBoundSpec())).toArray();
            router = PartitionRouter.createByHasher(strategy, datumArr, hasher, null/*use Long Comp*/);

        } else {
            throw new UnsupportedOperationException("partition strategy " + strategy);
        }

        return router;
    }

    public static PartitionBoundSpec sortListPartitionsAllValues(
        SearchDatumComparator cmp,
        PartitionBoundSpec bndSpec) {
        if (bndSpec.isSingleValue()) {
            throw new RuntimeException("should be multi-value bound");
        }

        PartitionBoundSpec newBndSpec = bndSpec.copy();
        newBndSpec.getMultiDatums().sort(cmp);
        return newBndSpec;
    }

    public PartitionIntFunction[] getPartFuncArr() {
        return partFuncArr;
    }

    public void setPartFuncArr(PartitionIntFunction[] partFuncArr) {
        this.partFuncArr = partFuncArr;
    }
}
