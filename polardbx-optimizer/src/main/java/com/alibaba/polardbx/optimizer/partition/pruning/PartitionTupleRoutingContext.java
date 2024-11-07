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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.parse.util.Pair;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class PartitionTupleRoutingContext {

    /**
     * dbName
     */
    protected String schemaName;

    /**
     * Logical table name
     */
    protected String tableName;

    /**
     * the partInfo of tableName
     */
    protected PartitionInfo partInfo;

    /**
     * The column meta list of a target tuple
     */
    protected List<ColumnMeta> targetRowColMetas;

    /**
     * The col metas of all-level partition （both partition and subpartiton）
     */
    protected FullPartColMetaInfo fullPartByColMeta;

    protected static class FullPartColMetaInfo {
        /**
         * the partition column metas of all-level-partition, its order is important!
         */
        protected List<ColumnMeta> fullPartColMetas;

        /**
         * The index mapping from full col meta to part col of partition definition
         * Each entry of list is :
         * key : 0: partition columns,  1: subpartitions columns
         * val : the index of col in (sub)partition columns list of original partition definition
         * Notice: if a subpart col already exists in partBy, its index mapping item of the pair of
         * fullColMetaToPartColMetaMapping will be ignore.
         * <pre>
         *      e.g
         *          the full part cols: A,C,D,E  (all part cols including both partBy and subpartBy)
         *          the part cols:      A,C,D
         *          the subPartCols:    D,E,C
         *          the mapping:   {0, 0}, {0,1}, {0,2}, {1,1}
         *  </pre>
         */
        protected List<Pair<Integer, Integer>> fullColMetaToPartColMetaMapping = new ArrayList<>();

        /**
         * the relRowDataType of all-level-partition columns based on the targetRowColMetas of target values
         */
        protected RelDataType fullPartColRelRowType;

        /**
         * the index of each all-level-partition columns in the targetRowColMetas, its order is important!
         * <p>
         * its order must be the same as the partition columns definitions.
         * <p>
         * <pre>
         *
         *  For example:
         *      target col metas : (b,c,d,a)
         *      target values : (1,100,1000,10000)
         *      partition columns:  (a,b)
         *      column index of target col metas: (1,2,3,4)
         *      column index of partition col metas: (4,1)
         *      target values of partition col metas: (10000,1)
         * </pre>
         */
        protected List<Integer> fullPartColIndexMappings = new ArrayList<>();

        public FullPartColMetaInfo() {
        }

        public List<ColumnMeta> getFullPartColMetas() {
            return fullPartColMetas;
        }

        public void setFullPartColMetas(List<ColumnMeta> fullPartColMetas) {
            this.fullPartColMetas = fullPartColMetas;
        }

        public RelDataType getFullPartColRelRowType() {
            return fullPartColRelRowType;
        }

        public void setFullPartColRelRowType(RelDataType fullPartColRelRowType) {
            this.fullPartColRelRowType = fullPartColRelRowType;
        }

        public List<Integer> getFullPartColIndexMappings() {
            return fullPartColIndexMappings;
        }

        public List<Pair<Integer, Integer>> getFullColMetaToPartColMetaMapping() {
            return fullColMetaToPartColMetaMapping;
        }

        public void setFullColMetaToPartColMetaMapping(
            List<Pair<Integer, Integer>> fullColMetaToPartColMetaMapping) {
            this.fullColMetaToPartColMetaMapping = fullColMetaToPartColMetaMapping;
        }

        public void setFullPartColIndexMappings(List<Integer> fullPartColIndexMappings) {
            this.fullPartColIndexMappings = fullPartColIndexMappings;
        }
    }

    protected PartitionTupleRoutingContext() {
    }

    public SqlCall createPartColDynamicParamAst() {

        SqlNode[] rowOpArr = new SqlNode[this.fullPartByColMeta.getFullPartColMetas().size()];
        SqlBasicCall rowAst = new SqlBasicCall(TddlOperatorTable.ROW, rowOpArr, SqlParserPos.ZERO);
        for (int i = 0; i < rowOpArr.length; i++) {
            rowOpArr[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);
        }
        SqlNode[] rowsAstOpArr = new SqlNode[1];
        rowsAstOpArr[0] = rowAst;
        return new SqlBasicCall(TddlOperatorTable.VALUES, rowsAstOpArr, SqlParserPos.ZERO);
    }

    public Parameters createPartColValueParametersByPartTupleAndSubPartTuple(List<List<Object>> partColTupleVals) {
        List<Object> targetRowValues = new ArrayList<>();
        List<Pair<Integer, Integer>> fullColToPartColMapping =
            this.fullPartByColMeta.getFullColMetaToPartColMetaMapping();
        for (int i = 0; i < fullColToPartColMapping.size(); i++) {
            Pair<Integer, Integer> mapInfo = fullColToPartColMapping.get(i);
            Integer partLevelFlag = mapInfo.getKey();
            Integer partColIdx = mapInfo.getValue();
            targetRowValues.add(partColTupleVals.get(partLevelFlag).get(partColIdx));
        }
        return createPartColValueParameters(targetRowValues, true);
    }

    public Parameters createPartColValueParameters(List<Object> targetRowValues,
                                                   boolean usePartColIndexMapping) {
        Map<Integer, ParameterContext> tmpParams = new HashMap<>();
        int fullPartColCnt = this.fullPartByColMeta.getFullPartColMetas().size();
        for (int j = 0; j < fullPartColCnt; j++) {
            Object val = null;
            if (usePartColIndexMapping) {
                Integer idxInTargetRowColMetas = this.fullPartByColMeta.getFullPartColIndexMappings().get(j);
                val = targetRowValues.get(idxInTargetRowColMetas);
            } else {
                val = targetRowValues.get(j);
            }
            ParameterContext pc = new ParameterContext(ParameterMethod.setObject1, new Object[] {j + 1, val});
            tmpParams.put(j + 1, pc);
        }
        Parameters allParams = new Parameters(tmpParams);
        return allParams;
    }

    /**
     * Compute the col index mapping
     * from
     * partCol meta definition order
     * to
     * the col meta definition order of insert values,
     * and output the result into outputPartColIdxList and outputPartColRelRowTypeList
     *
     * <pre>
     *  outputPartColIdxList: the index mapping of a col on the col meta definition order of insert values
     *  e.g.
     *      insert values col order: insert into t1 (b, c, a) ....
     *      insert values col index:                 0  1  2
     *      part col definition order : a,  b
     *      the outputPartColIdxList:   2,  0
     * </pre>
     */
    protected void computePartColIndexMappings(List<ColumnMeta> fullPartColMetas,
                                               List<ColumnMeta> targetTupleRowColMetas,
                                               List<Integer> outputPartColIdxList,
                                               List<RelDataTypeField> outputPartColRelRowTypeList) {
        for (ColumnMeta columnMeta : fullPartColMetas) {
            String partColName = columnMeta.getName();
            if (partColName.contains(".")) {
                partColName = partColName.split("\\.")[1]; // 避免转义
            }

            List<ColumnMeta> targetColumnList = targetTupleRowColMetas;
            int index = -1;
            for (int i = 0; i < targetColumnList.size(); i++) {
                String colName = targetColumnList.get(i).getField().getOriginColumnName();
                if (colName.equalsIgnoreCase(partColName)) {
                    index = i;
                    break;
                }
            }
            // if it's absent, it's using default value
            if (index < 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL_UNKNOWN_COLUMN,
                    "No found any partition column of " + partColName);
            }
            outputPartColIdxList.add(index);
            RelDataTypeField relDataTypeField =
                new RelDataTypeFieldImpl(columnMeta.getName(), index, columnMeta.getField().getRelType());
            outputPartColRelRowTypeList.add(relDataTypeField);
        }
    }

    protected FullPartColMetaInfo buildFullPartColMetaInfo(PartitionByDefinition partBy) {
        if (partBy == null) {
            return null;
        }
        FullPartColMetaInfo fullPartColMetaInfo = new FullPartColMetaInfo();

        List<ColumnMeta> fullPartColMetas = partBy.getFullPartitionColumnMetas();
        List<Integer> outputFullPartColIdxList = new ArrayList<>();
        List<RelDataTypeField> outputFullPartColRelRowTypeList = new ArrayList<>();
        computePartColIndexMappings(fullPartColMetas, targetRowColMetas, outputFullPartColIdxList,
            outputFullPartColRelRowTypeList);
        RelRecordType partColRelRowType = new RelRecordType(outputFullPartColRelRowTypeList);
        fullPartColMetaInfo.setFullPartColMetas(fullPartColMetas);
        fullPartColMetaInfo.setFullPartColRelRowType(partColRelRowType);
        fullPartColMetaInfo.setFullPartColIndexMappings(outputFullPartColIdxList);

        /**
         * Construct the partColName to partCol definition index for both partBy and subpartBy
         */
        Map<String, Integer> partColIdxMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, Integer> subPartColIdxMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<String> partCols = partBy.getPartitionColumnNameList();
        for (int i = 0; i < partCols.size(); i++) {
            partColIdxMap.put(partCols.get(i), i);
        }
        if (partBy.getSubPartitionBy() != null) {
            List<String> subPartCols = partBy.getSubPartitionBy().getPartitionColumnNameList();
            for (int i = 0; i < subPartCols.size(); i++) {
                subPartColIdxMap.put(subPartCols.get(i), i);
            }
        }

        /**
         * C
         */
        List<Pair<Integer, Integer>> fullColIndexMapping = new ArrayList<>();
        for (int i = 0; i < fullPartColMetas.size(); i++) {
            ColumnMeta cm = fullPartColMetas.get(i);
            String name = cm.getName();
            if (partColIdxMap.containsKey(name)) {
                fullColIndexMapping.add(new Pair<>(0, partColIdxMap.get(name)));
            } else if (subPartColIdxMap.containsKey(name)) {
                fullColIndexMapping.add(new Pair<>(1, subPartColIdxMap.get(name)));
            }
        }
        fullPartColMetaInfo.setFullColMetaToPartColMetaMapping(fullColIndexMapping);
        return fullPartColMetaInfo;

    }

    protected void initTupleRoutingContext() {
        this.fullPartByColMeta = buildFullPartColMetaInfo(partInfo.getPartitionBy());
    }

    public static PartitionTupleRoutingContext buildPartitionTupleRoutingContext(String schemaName,
                                                                                 String tableName,
                                                                                 PartitionInfo partInfo,
                                                                                 List<ColumnMeta> targetValuesColMetas) {
        PartitionTupleRoutingContext ctx = new PartitionTupleRoutingContext();
        ctx.schemaName = schemaName;
        ctx.tableName = tableName;
        ctx.partInfo = partInfo;
        ctx.targetRowColMetas = targetValuesColMetas;
        ctx.initTupleRoutingContext();
        return ctx;
    }

    public RelDataType getPartColRelRowType() {
        return this.fullPartByColMeta.getFullPartColRelRowType();
    }

    public List<Integer> getPartColIndexMappings() {
        return this.fullPartByColMeta.getFullPartColIndexMappings();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }
}
