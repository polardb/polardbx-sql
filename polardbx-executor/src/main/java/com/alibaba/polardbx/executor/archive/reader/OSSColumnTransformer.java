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

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.BlockConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.eclipse.jetty.util.StringUtil;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * a transformer used to transform source column in orc file to target column
 */
public class OSSColumnTransformer {
    // column metas used in orc file
    private final List<ColumnMeta> sourceColumnMetas;

    // default column of missing column in orc file
    private final List<ColumnMeta> initColumnMetas;

    // column metas needed in the query
    private final List<ColumnMeta> targetColumnMetas;

    private final List<Timestamp> timestamps;

    private final List<TypeComparison> comparisonResults;

    private final Map<String, Integer> targetColumnMap;

    /**
     * the location of target columns in column vectors, null if not presented
     */
    private final List<Integer> locInOrc;

    public OSSColumnTransformer(List<ColumnMeta> columnMetas,
                                List<ColumnMeta> fileColumnMetas,
                                List<ColumnMeta> initColumnMetas,
                                List<Timestamp> timestamps) {
        Preconditions.checkArgument(columnMetas.size() == fileColumnMetas.size(),
            "target and source should have the same size");
        this.sourceColumnMetas = fileColumnMetas;
        this.initColumnMetas = initColumnMetas;
        this.targetColumnMetas = columnMetas;
        this.timestamps = timestamps;
        this.locInOrc = new ArrayList<>(sourceColumnMetas.size());
        int cnt = 0;
        for (ColumnMeta meta : sourceColumnMetas) {
            locInOrc.add((meta == null) ? null : cnt++);
        }
        this.targetColumnMap = Maps.newTreeMap(String::compareToIgnoreCase);
        for (int i = 0; i < targetColumnMetas.size(); i++) {
            this.targetColumnMap.put(targetColumnMetas.get(i).getName().toLowerCase(), i);
        }
        this.comparisonResults = new ArrayList<>(sourceColumnMetas.size());
        for (int i = 0; i < sourceColumnMetas.size(); i++) {
            TypeComparison type = OSSColumnTransformerUtil.compare(targetColumnMetas.get(i), sourceColumnMetas.get(i));
            if (type == TypeComparison.MISSING_EQUAL) {
                type = OSSColumnTransformerUtil.compare(targetColumnMetas.get(i), initColumnMetas.get(i));
                this.comparisonResults.add(OSSColumnTransformerUtil.defaultColumnCompare(type));
            } else {
                this.comparisonResults.add(type);
            }
        }
    }

    String[] getTargetColumns() {
        String[] columns = new String[sourceColumnMetas.size()];
        for (int i = 0; i < sourceColumnMetas.size(); i++) {
            columns[i] = sourceColumnMetas.get(i).getName();
        }
        return columns;
    }

    List<ColumnProvider<?>> getTargetColumnProvides() {
        return targetColumnMetas.stream()
            .map(ColumnProviders::getProvider).collect(Collectors.toList());
    }

    public ColumnMeta getSourceColumnMeta(int i) {
        return sourceColumnMetas.get(i);
    }

    public ColumnMeta getTargetColumnMeta(int i) {
        return targetColumnMetas.get(i);
    }

    public ColumnMeta getInitColumnMeta(int i) {
        return initColumnMetas.get(i);
    }

    public Timestamp getTimeStamp(int i) {
        return timestamps.get(i);
    }

    /**
     * get the position of a column in target column list
     *
     * @param columnName column to find
     * @return rank in target list, null if not found
     */
    public int getTargetColumnRank(String columnName) {
        Preconditions.checkArgument(!StringUtil.isEmpty(columnName), "column Name is Empty!");

        Integer rank = targetColumnMap.get(columnName);
        Preconditions.checkArgument(rank != null, "Column " + columnName + " missing");
        return rank.intValue();
    }

    public TypeComparison compare(String columnName) {
        int rank = getTargetColumnRank(columnName);
        return comparisonResults.get(rank);
    }

    public TypeComparison getCompareResult(int fieldId) {
        return comparisonResults.get(fieldId);
    }

    public ColumnMeta getTargetColumnMeta(String column) {

        return targetColumnMetas.stream()
            .filter(x -> x.getName().equals(column)).findFirst().get();
    }

    public Integer getLocInOrc(int loc) {
        return locInOrc.get(loc);
    }

    /**
     * fill in default value of target column.
     *
     * @param targetDataType target type
     * @param targetColumnMeta target column meta info
     * @param rowCount default value row count
     * @param context ExecutionContext used to build BlockBuilder
     * @return a block containing expected rows of default value
     */
    public static Block fillDefaultValue(DataType targetDataType,
                                         ColumnMeta targetColumnMeta,
                                         Timestamp ts,
                                         int rowCount,
                                         ExecutionContext context) {
        // use block to store the default value.
        BlockBuilder defaultValueBlockBuilder = BlockBuilders.create(targetDataType, context);
        if (targetDataType instanceof TimestampType) {
            if ("current_timestamp".equalsIgnoreCase(targetColumnMeta.getField().getDefault())) {
                defaultValueBlockBuilder.writeObject(targetDataType.convertFrom(ts));
            } else {
                defaultValueBlockBuilder.writeObject(
                    targetDataType.convertFrom(targetColumnMeta.getField().getDefault()));
            }
        } else {
            defaultValueBlockBuilder.writeObject(targetDataType.convertFrom(targetColumnMeta.getField().getDefault()));
        }
        Block defaultValueBlock = defaultValueBlockBuilder.build();

        BlockBuilder blockBuilder = BlockBuilders.create(targetDataType, context);
        for (int j = 0; j < rowCount; j++) {
            // continually append block builder from 0 position of default value block.
            defaultValueBlock.writePositionTo(0, blockBuilder);
        }
        return blockBuilder.build();
    }

    /**
     * fill in default value of source column and transform to target column type.
     *
     * @param targetColumnMeta target column meta info
     * @param sourceColumnMeta source column meta info
     * @param rowCount default value row count
     * @param context ExecutionContext used to build BlockBuilder
     * @return a block containing expected rows of default value
     */
    public static Block fillDefaultValueAndTransform(ColumnMeta targetColumnMeta,
                                                     ColumnMeta sourceColumnMeta,
                                                     int rowCount,
                                                     ExecutionContext context) {
        DataType targetDataType = targetColumnMeta.getDataType();
        Object targetDefaultValue = targetDataType.convertFrom(sourceColumnMeta.getField().getDefault());

        // use block to store the default value.
        BlockBuilder defaultValueBlockBuilder = BlockBuilders.create(targetDataType, context);
        defaultValueBlockBuilder.writeObject(targetDefaultValue);
        Block defaultValueBlock = defaultValueBlockBuilder.build();

        BlockBuilder blockBuilder = BlockBuilders.create(targetDataType, context);
        for (int j = 0; j < rowCount; j++) {
            // continually append block builder from 0 position of default value block.
            defaultValueBlock.writePositionTo(0, blockBuilder);
        }
        return blockBuilder.build();
    }

    /**
     * given targetColumnMeta, sourceColumnMeta and columnVector using datatype from sourceColumnMeta
     * generate result of target datatype
     *
     * @param targetColumnMeta target column meta info
     * @param sourceColumnMeta source column meta info
     * @param columnVector data read from oss
     * @param selection selection array of columnVector, null if without filter
     * @param selSize length of selection array
     * @param rowCount length of columnVectors, useful when selection is null
     * @param sessionProperties column vector related information
     * @param context ExecutionContext
     * @return a block with targetDataType
     */
    public static Block transformDataType(ColumnMeta targetColumnMeta,
                                          ColumnMeta sourceColumnMeta,
                                          ColumnVector columnVector,
                                          int[] selection,
                                          int selSize,
                                          int rowCount,
                                          SessionProperties sessionProperties,
                                          ExecutionContext context) {

        DataType targetDataType = targetColumnMeta.getField().getDataType();
        DataType sourceDataType = sourceColumnMeta.getField().getDataType();
        ColumnProvider sourceColumnProvider =
            ColumnProviders.getProvider(sourceDataType);
        BlockBuilder sourceBlockBuilder = BlockBuilders.create(sourceDataType, context);

        sourceColumnProvider.transform(
            columnVector,
            sourceBlockBuilder,
            selection,
            selSize,
            0,
            rowCount,
            sessionProperties
        );
        Block sourceBlock = sourceBlockBuilder.build();
        BlockConverter converter = Converters.createBlockConverter(sourceDataType, targetDataType, context);
        return converter.apply(sourceBlock);
    }

}
