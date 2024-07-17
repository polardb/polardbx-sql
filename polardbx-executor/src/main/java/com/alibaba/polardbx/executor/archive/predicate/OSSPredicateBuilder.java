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

package com.alibaba.polardbx.executor.archive.predicate;

import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64Utils;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.reader.TypeComparison;
import com.alibaba.polardbx.executor.operator.util.minmaxfilter.MinMaxFilter;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.orc.impl.TypeUtils;
import org.apache.orc.sarg.PredicateLeaf;
import org.apache.orc.sarg.SearchArgument;
import org.apache.orc.sarg.SearchArgumentFactory;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OSSPredicateBuilder extends RexVisitorImpl<Boolean> {
    SearchArgument.Builder builder;
    Set<String> columns;
    Parameters parameters;
    List<RelDataTypeField> filterFields;
    Map<Integer, BloomFilterInfo> bloomFilterInfoMap;
    List<RelDataTypeField> runtimeFilterFields;
    TableMeta tableMeta;
    SessionProperties sessionProperties;

    OSSColumnTransformer ossColumnTransformer;

    boolean enableAggPruner;

    OSSOrcFileMeta fileMeta;

    public OSSPredicateBuilder(Parameters parameters, List<RelDataTypeField> filterFields,
                               Map<Integer, BloomFilterInfo> bloomFilterInfoMap,
                               List<RelDataTypeField> runtimeFilterFields, TableMeta tableMeta,
                               SessionProperties sessionProperties,
                               OSSColumnTransformer ossColumnTransformer,
                               OSSOrcFileMeta fileMeta) {
        super(false);
        this.builder = SearchArgumentFactory.newBuilder();
        this.parameters = parameters;
        this.columns = new HashSet<>();
        this.filterFields = filterFields;
        this.bloomFilterInfoMap = bloomFilterInfoMap;
        this.runtimeFilterFields = runtimeFilterFields;
        this.tableMeta = tableMeta;
        this.sessionProperties = sessionProperties;

        this.ossColumnTransformer = ossColumnTransformer;
        this.enableAggPruner = true;
        this.fileMeta = fileMeta;
    }

    String getFieldId(String targetColumn) {
        TypeComparison result = ossColumnTransformer.compare(targetColumn);

        if (result == TypeComparison.IS_EQUAL_YES) {
            return tableMeta.getColumnFieldId(targetColumn);
        }

        // Todo: support filter for different type!
        if (result == TypeComparison.IS_EQUAL_NO) {
            this.enableAggPruner = false;
            return null;
        }

        // new column after orc file
        if (TypeComparison.isMissing(result)) {
            this.enableAggPruner = false;
            return null;
        }

        return null;
    }

    public String[] columns() {
        return this.columns.toArray(new String[] {});
    }

    public SearchArgument build() {
        return builder.build();
    }

    @Override
    public Boolean visitCall(RexCall call) {
        boolean valid = false;
        SqlKind kind = call.op.kind;
        switch (kind) {
        case AND:
            valid = visitAnd(call);
            break;
        case OR:
            valid = visitOr(call);
            break;
        case NOT:
            valid = visitNot(call);
            break;
        case EQUALS:
            valid = visitBinary(call, (s, t, objects) -> builder.equals(s, t, objects[0]));
            break;
        case LESS_THAN:
            valid = visitBinary(call, (s, t, objects) -> builder.lessThan(s, t, objects[0]));
            break;
        case LESS_THAN_OR_EQUAL:
            valid = visitBinary(call, (s, t, objects) -> builder.lessThanEquals(s, t, objects[0]));
            break;
        case GREATER_THAN:
            valid = visitBinary(call, (s, t, objects) -> builder.greaterThan(s, t, objects[0]));
            break;
        case GREATER_THAN_OR_EQUAL:
            valid = visitBinary(call, (s, t, objects) -> builder.greaterThanEquals(s, t, objects[0]));
            break;
        case BETWEEN:
            valid = visitBetween(call);
            break;
        case IN:
            valid = visitIn(call);
            break;
        case IS_NULL:
            valid = visitIsNull(call);
            break;
        case RUNTIME_FILTER:
            valid = visitRuntimeFilter(call);
            break;
        }
        return valid;
    }

    private boolean visitBinary(RexCall call,
                                TriFunction<String, PredicateLeaf.Type, Object[], SearchArgument.Builder> applier) {
        RexInputRef field;
        RexDynamicParam dynamicParam;
        if (call.getOperands().get(0) instanceof RexInputRef && call.getOperands().get(1) instanceof RexDynamicParam) {
            field = (RexInputRef) call.getOperands().get(0);
            dynamicParam = (RexDynamicParam) call.getOperands().get(1);
        } else if (call.getOperands().get(1) instanceof RexInputRef && call.getOperands()
            .get(0) instanceof RexDynamicParam
            && call.getOperator().getKind() == SqlKind.EQUALS) {
            field = (RexInputRef) call.getOperands().get(1);
            dynamicParam = (RexDynamicParam) call.getOperands().get(0);
        } else {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return false;
        }

        Object value = parameters.getCurrentParameter().get(dynamicParam.getIndex() + 1).getValue();

        String columnName = filterFields.get(field.getIndex()).getName();
        SqlTypeName typeName = field.getType().getSqlTypeName();
        DataType dataType = tableMeta.getColumn(columnName).getDataType();
        columnName = getFieldId(columnName);
        if (columnName == null) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }

        columns.add(columnName);

        // should we prune the sort key column instead of original col
        String redundantColumn = null;
        // precise data type info from table meta
        DataType preciseDataType = null;
        if (fileMeta.getColumnNameToIdx(OrcMetaUtils.redundantColumnOf(columnName)) != null) {
            redundantColumn = OrcMetaUtils.redundantColumnOf(columnName);
            columns.add(redundantColumn);

            Integer rank = ossColumnTransformer.getTargetColumnRank(filterFields.get(field.getIndex()).getName());
            preciseDataType = ossColumnTransformer.getSourceColumnMeta(rank).getDataType();
            if (!(preciseDataType instanceof SliceType)) {
                // sort key unsupported
                return false;
            }
        }

        switch (typeName) {
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT:
        case SMALLINT_UNSIGNED:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case INTEGER_UNSIGNED:
        case BIGINT:
            if (value instanceof Number) {
                applier.apply(columnName, PredicateLeaf.Type.LONG, new Object[] {((Number) value).longValue()});
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case BIGINT_UNSIGNED:
            if (value instanceof Number) {
                applier.apply(columnName, PredicateLeaf.Type.LONG,
                    new Object[] {((Number) value).longValue() ^ UInt64Utils.FLIP_MASK});
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case DOUBLE:
        case FLOAT:
            if (value instanceof Number) {
                applier.apply(columnName, PredicateLeaf.Type.FLOAT, new Object[] {((Number) value).doubleValue()});
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case VARCHAR:
        case CHAR:
            if (value instanceof String && redundantColumn != null && preciseDataType != null) {
                // use sort key to prune
                String pruneValue = makeSortKeyString(value, (SliceType) preciseDataType);
                applier.apply(redundantColumn, PredicateLeaf.Type.STRING, new Object[] {pruneValue});
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case DECIMAL:
            if (!(value instanceof Number)) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            Decimal decimal = Decimal.fromString(value.toString());
            if (fileMeta.isEnableDecimal64() && TypeUtils.isDecimal64Precision(dataType.getPrecision())) {
                // passing an approximate value for pruning
                long longVal = decimal.unscaleInternal(dataType.getScale());
                applier.apply(columnName, PredicateLeaf.Type.LONG, new Object[] {longVal});
                return true;
            }
            byte[] bytes = OssOrcFilePruner.decimalToBin(decimal.getDecimalStructure(),
                dataType.getPrecision(), dataType.getScale());
            applier.apply(columnName, PredicateLeaf.Type.STRING,
                new Object[] {new String(bytes, StandardCharsets.UTF_8)});
            return true;
        case TIMESTAMP:
            MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                value,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (mysqlDateTime == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            TimeParseStatus timeParseStatus = new TimeParseStatus();
            MySQLTimeVal timeVal =
                MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime, timeParseStatus,
                    sessionProperties.getTimezone());
            if (timeVal == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            applier.apply(columnName, PredicateLeaf.Type.LONG, new Object[] {XResultUtil.timeValToLong(timeVal)});
            return true;
        case DATE:
        case DATETIME:
            MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(
                value,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (t == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            long packed = TimeStorage.writeTimestamp(t);
            applier.apply(columnName, PredicateLeaf.Type.LONG, new Object[] {packed});
            return true;
        case TIME:
            MysqlDateTime time = DataTypeUtil.toMySQLDatetimeByFlags(
                value,
                Types.TIME,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (time == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            long packedTime = TimeStorage.writeTime(time);
            applier.apply(columnName, PredicateLeaf.Type.LONG, new Object[] {packedTime});
            return true;
        default:
        }

        return false;
    }

    private boolean visitBetween(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return false;
        }
        if (!(call.getOperands().get(1) instanceof RexDynamicParam)) {
            return false;
        }
        if (!(call.getOperands().get(2) instanceof RexDynamicParam)) {
            return false;
        }
        RexInputRef field = (RexInputRef) call.getOperands().get(0);
        RexDynamicParam dynamicParam1 = (RexDynamicParam) call.getOperands().get(1);
        RexDynamicParam dynamicParam2 = (RexDynamicParam) call.getOperands().get(2);

        Object value1 = parameters.getCurrentParameter().get(dynamicParam1.getIndex() + 1).getValue();
        Object value2 = parameters.getCurrentParameter().get(dynamicParam2.getIndex() + 1).getValue();

        String columnName = filterFields.get(field.getIndex()).getName();
        SqlTypeName typeName = field.getType().getSqlTypeName();
        DataType dataType = tableMeta.getColumn(columnName).getDataType();
        columnName = getFieldId(columnName);
        if (columnName == null) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }
        columns.add(columnName);

        // should we prune the sort key column instead of original col
        String redundantColumn = null;
        // precise data type info from table meta
        DataType preciseDataType = null;
        if (fileMeta.getColumnNameToIdx(OrcMetaUtils.redundantColumnOf(columnName)) != null) {
            redundantColumn = OrcMetaUtils.redundantColumnOf(columnName);
            columns.add(redundantColumn);

            Integer rank = ossColumnTransformer.getTargetColumnRank(filterFields.get(field.getIndex()).getName());
            preciseDataType = ossColumnTransformer.getSourceColumnMeta(rank).getDataType();
            if (!(preciseDataType instanceof SliceType)) {
                // sort key unsupported
                return false;
            }
        }

        switch (typeName) {
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT:
        case SMALLINT_UNSIGNED:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case INTEGER_UNSIGNED:
        case BIGINT:
            if (value1 instanceof Number && value2 instanceof Number) {
                builder.between(columnName, PredicateLeaf.Type.LONG, ((Number) value1).longValue(),
                    ((Number) value2).longValue());
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case BIGINT_UNSIGNED:
            if (value1 instanceof Number && value2 instanceof Number) {
                builder.between(columnName, PredicateLeaf.Type.LONG,
                    ((Number) value1).longValue() ^ UInt64Utils.FLIP_MASK,
                    ((Number) value2).longValue() ^ UInt64Utils.FLIP_MASK);
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case DOUBLE:
        case FLOAT:
            if (value1 instanceof Number && value2 instanceof Number) {
                builder.between(columnName, PredicateLeaf.Type.FLOAT, ((Number) value1).doubleValue(),
                    ((Number) value2).doubleValue());
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case VARCHAR:
        case CHAR:
            if (value1 instanceof String && value2 instanceof String && redundantColumn != null
                && preciseDataType != null) {
                // use sort key to prune
                String pruneValue1 = makeSortKeyString(value1, (SliceType) preciseDataType);
                String pruneValue2 = makeSortKeyString(value2, (SliceType) preciseDataType);

                builder.between(redundantColumn, PredicateLeaf.Type.STRING, pruneValue1, pruneValue2);
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case DECIMAL:
            if (!(value1 instanceof Number && value2 instanceof Number)) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            Decimal decimal1 = Decimal.fromString(value1.toString());
            Decimal decimal2 = Decimal.fromString(value2.toString());
            if (fileMeta.isEnableDecimal64() && TypeUtils.isDecimal64Precision(dataType.getPrecision())) {
                // passing approximate values with wider range for pruning
                long longVal1 = decimal1.unscaleInternal(dataType.getScale());
                long longVal2 = decimal2.unscaleInternal(dataType.getScale());
                builder.between(columnName, PredicateLeaf.Type.LONG, longVal1, longVal2);
                return true;
            }
            byte[] bytes1 =
                OssOrcFilePruner.decimalToBin(decimal1.getDecimalStructure(),
                    dataType.getPrecision(), dataType.getScale());
            byte[] bytes2 =
                OssOrcFilePruner.decimalToBin(decimal2.getDecimalStructure(),
                    dataType.getPrecision(), dataType.getScale());

            builder.between(columnName, PredicateLeaf.Type.STRING, new String(bytes1, StandardCharsets.UTF_8),
                new String(bytes2, StandardCharsets.UTF_8));
            return true;
        case TIMESTAMP:
            MysqlDateTime mysqlDateTime1 = DataTypeUtil.toMySQLDatetimeByFlags(
                value1,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (mysqlDateTime1 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            TimeParseStatus timeParseStatus1 = new TimeParseStatus();
            MySQLTimeVal timeVal1 =
                MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime1, timeParseStatus1,
                    sessionProperties.getTimezone());
            if (timeVal1 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }

            MysqlDateTime mysqlDateTime2 = DataTypeUtil.toMySQLDatetimeByFlags(
                value2,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (mysqlDateTime2 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            TimeParseStatus timeParseStatus2 = new TimeParseStatus();
            MySQLTimeVal timeVal2 =
                MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime2, timeParseStatus2,
                    sessionProperties.getTimezone());
            if (timeVal2 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }

            builder.between(columnName, PredicateLeaf.Type.LONG, XResultUtil.timeValToLong(timeVal1),
                XResultUtil.timeValToLong(timeVal2));
            return true;
        case DATE:
        case DATETIME:
            MysqlDateTime t1 = DataTypeUtil.toMySQLDatetimeByFlags(
                value1,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (t1 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            long packed1 = TimeStorage.writeTimestamp(t1);
            MysqlDateTime t2 = DataTypeUtil.toMySQLDatetimeByFlags(
                value2,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (t2 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            long packed2 = TimeStorage.writeTimestamp(t2);
            builder.between(columnName, PredicateLeaf.Type.LONG, packed1, packed2);
            return true;
        case TIME:
            MysqlDateTime time1 = DataTypeUtil.toMySQLDatetimeByFlags(
                value1,
                Types.TIME,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (time1 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            long packedTime1 = TimeStorage.writeTimestamp(time1);
            MysqlDateTime time2 = DataTypeUtil.toMySQLDatetimeByFlags(
                value2,
                Types.TIME,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (time2 == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            long packedTime2 = TimeStorage.writeTimestamp(time2);
            builder.between(columnName, PredicateLeaf.Type.LONG, packedTime1, packedTime2);
            return true;
        default:
        }

        return false;
    }

    private boolean checkInClass(List<Object> objects, Class clazz) {
        for (Object obj : objects) {
            if (!clazz.isInstance(obj)) {
                return false;
            }
        }
        return true;
    }

    private boolean visitIn(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return false;
        }

        if (!(call.getOperands().get(1) instanceof RexCall && call.getOperands().get(1).isA(SqlKind.ROW))) {
            return false;
        }

        RexInputRef field = (RexInputRef) call.getOperands().get(0);
        RexCall inRexCall = ((RexCall) call.getOperands().get(1));

        if (inRexCall.getOperands().size() > 1) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }
        if (!(inRexCall.getOperands().get(0) instanceof RexDynamicParam)) {
            return false;
        }
        RexDynamicParam para = (RexDynamicParam) inRexCall.getOperands().get(0);

        Object value = parameters.getCurrentParameter().get(para.getIndex() + 1).getValue();
        if (!(value instanceof RawString)) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }
        List<Object> paramList = ((RawString) value).getObjList();
        if (paramList.size() > 10) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }

        String columnName = filterFields.get(field.getIndex()).getName();
        DataType dataType = tableMeta.getColumn(columnName).getDataType();
        SqlTypeName typeName = field.getType().getSqlTypeName();
        columnName = getFieldId(columnName);
        if (columnName == null) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }
        columns.add(columnName);

        // should we prune the sort key column instead of original col
        String redundantColumn = null;
        // precise data type info from table meta
        DataType preciseDataType = null;
        if (fileMeta.getColumnNameToIdx(OrcMetaUtils.redundantColumnOf(columnName)) != null) {
            redundantColumn = OrcMetaUtils.redundantColumnOf(columnName);
            columns.add(redundantColumn);

            Integer rank = ossColumnTransformer.getTargetColumnRank(filterFields.get(field.getIndex()).getName());
            preciseDataType = ossColumnTransformer.getSourceColumnMeta(rank).getDataType();
            if (!(preciseDataType instanceof SliceType)) {
                // sort key unsupported
                return false;
            }
        }
        switch (typeName) {
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT:
        case SMALLINT_UNSIGNED:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case INTEGER_UNSIGNED:
        case BIGINT:
            if (checkInClass(paramList, Number.class)) {
                List<Object> newPara = Lists.newArrayListWithCapacity(paramList.size());
                for (Object obj : paramList) {
                    newPara.add(((Number) obj).longValue());
                }
                buildIn(columnName, PredicateLeaf.Type.LONG, newPara);
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case BIGINT_UNSIGNED:
            if (checkInClass(paramList, Number.class)) {
                List<Object> newPara = Lists.newArrayListWithCapacity(paramList.size());
                for (Object obj : paramList) {
                    newPara.add(((Number) obj).longValue() ^ UInt64Utils.FLIP_MASK);
                }
                buildIn(columnName, PredicateLeaf.Type.LONG, newPara);
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case DOUBLE:
        case FLOAT:
            if (checkInClass(paramList, Number.class)) {
                List<Object> newPara = Lists.newArrayListWithCapacity(paramList.size());
                for (Object obj : paramList) {
                    newPara.add(((Number) obj).doubleValue());
                }
                buildIn(columnName, PredicateLeaf.Type.FLOAT, newPara);
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case VARCHAR:
        case CHAR:
            if (redundantColumn != null && preciseDataType != null && checkInClass(paramList, String.class)) {
                List<Object> newPara = Lists.newArrayListWithCapacity(paramList.size());
                final SliceType sliceType = (SliceType) preciseDataType;
                for (Object obj : paramList) {
                    newPara.add(makeSortKeyString(obj, sliceType));
                }
                buildIn(redundantColumn, PredicateLeaf.Type.STRING, newPara);
            } else {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            }
            return true;
        case DECIMAL:
            if (!checkInClass(paramList, Number.class)) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            List<Object> newPara = Lists.newArrayListWithCapacity(paramList.size());
            if (fileMeta.isEnableDecimal64() && TypeUtils.isDecimal64Precision(dataType.getPrecision())) {
                for (Object obj : paramList) {
                    Decimal decimal = Decimal.fromString(obj.toString());
                    long longVal1 = decimal.unscaleInternal(dataType.getScale());
                    newPara.add(longVal1);
                }
                buildIn(columnName, PredicateLeaf.Type.LONG, newPara);
                return true;
            }

            for (Object obj : paramList) {
                byte[] bytes =
                    OssOrcFilePruner.decimalToBin(Decimal.fromString(obj.toString()).getDecimalStructure(),
                        dataType.getPrecision(), dataType.getScale());
                newPara.add(new String(bytes, StandardCharsets.UTF_8));
            }
            buildIn(columnName, PredicateLeaf.Type.STRING, newPara);
            return true;
        case TIMESTAMP:
            List<Object> timestampLongValueParams = Lists.newArrayListWithCapacity(paramList.size());
            for (Object obj : paramList) {
                MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                    obj,
                    TimeParserFlags.FLAG_TIME_FUZZY_DATE);
                if (mysqlDateTime == null) {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                    return true;
                }
                TimeParseStatus timeParseStatus = new TimeParseStatus();
                MySQLTimeVal timeVal =
                    MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime, timeParseStatus,
                        sessionProperties.getTimezone());
                if (timeVal == null) {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                    return true;
                }
                timestampLongValueParams.add(XResultUtil.timeValToLong(timeVal));
            }
            buildIn(columnName, PredicateLeaf.Type.LONG, timestampLongValueParams);
            return true;
        case DATE:
        case DATETIME:
            List<Object> packedParams = Lists.newArrayListWithCapacity(paramList.size());
            for (Object obj : paramList) {
                MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                    obj,
                    TimeParserFlags.FLAG_TIME_FUZZY_DATE);
                if (mysqlDateTime == null) {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                    return true;
                }
                long packed = TimeStorage.writeTimestamp(mysqlDateTime);
                packedParams.add(packed);
            }
            buildIn(columnName, PredicateLeaf.Type.LONG, packedParams);
            return true;
        case TIME:
            List<Object> packedTimeParams = Lists.newArrayListWithCapacity(paramList.size());
            for (Object obj : paramList) {
                MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                    obj,
                    Types.TIME,
                    TimeParserFlags.FLAG_TIME_FUZZY_DATE);
                if (mysqlDateTime == null) {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                    return true;
                }
                long packed = TimeStorage.writeTimestamp(mysqlDateTime);
                packedTimeParams.add(packed);
            }
            buildIn(columnName, PredicateLeaf.Type.LONG, packedTimeParams);
            return true;
        default:
        }

        return false;
    }

    private void buildIn(String columnName, PredicateLeaf.Type type, List<Object> paramList) {
        switch (paramList.size()) {
        case 1:
            builder.in(columnName, type, paramList.get(0));
            break;
        case 2:
            builder.in(columnName, type, paramList.get(0), paramList.get(1));
            break;
        case 3:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2));
            break;
        case 4:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3));
            break;
        case 5:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3),
                paramList.get(4));
            break;
        case 6:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3),
                paramList.get(4), paramList.get(5));
            break;
        case 7:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3),
                paramList.get(4), paramList.get(5), paramList.get(6));
            break;
        case 8:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3),
                paramList.get(4), paramList.get(5), paramList.get(6), paramList.get(7));
            break;
        case 9:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3),
                paramList.get(4), paramList.get(5), paramList.get(6), paramList.get(7), paramList.get(8));
            break;
        case 10:
            builder.in(columnName, type, paramList.get(0), paramList.get(1), paramList.get(2), paramList.get(3),
                paramList.get(4), paramList.get(5), paramList.get(6), paramList.get(7), paramList.get(8),
                paramList.get(9));
            break;
        }
    }

    private boolean visitAnd(RexCall call) {
        builder.startAnd();
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexCall) {
                boolean valid = operand.accept(this);
                if (!valid) {
                    return false;
                }
            }
        }
        builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
        builder.end();
        return true;
    }

    private boolean visitOr(RexCall call) {
        for (RexNode node : call.getOperands()) {
            if (!(node instanceof RexCall)) {
                return false;
            }
        }
        builder.startOr();
        for (RexNode operand : call.getOperands()) {
            if (!operand.accept(this)) {
                return false;
            }
        }
        builder.end();
        return true;
    }

    private boolean visitNot(RexCall call) {
        for (RexNode node : call.getOperands()) {
            if (!(node instanceof RexCall)) {
                return false;
            }
        }

        RexCall rightCall = (RexCall) call.getOperands().get(0);

        builder.startNot();
        if (!rightCall.accept(this)) {
            return false;
        }
        builder.end();
        return true;
    }

    private boolean visitIsNull(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }

        RexInputRef rexInputRef = (RexInputRef) call.getOperands().get(0);

        String columnName = filterFields.get(rexInputRef.getIndex()).getName();

        columnName = getFieldId(columnName);
        if (columnName == null) {
            builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
            return true;
        }
        builder.isNull(columnName, PredicateLeaf.Type.BOOLEAN);

        return true;
    }

    private boolean visitRuntimeFilter(RexCall call) {
        int runtimeFilterId = ((SqlRuntimeFilterFunction) call.getOperator()).getId();
        BloomFilterInfo bloomFilterInfo = bloomFilterInfoMap.get(runtimeFilterId);

        List<MinMaxFilter> minMaxFilters =
            bloomFilterInfo.getMinMaxFilterInfoList().stream().map(x -> MinMaxFilter.from(x))
                .collect(Collectors.toList());

        for (int i = 0; i < call.getOperands().size(); i++) {
            if (!(call.getOperands().get(i) instanceof RexInputRef)) {
                return false;
            }
            MinMaxFilter minMaxFilter = minMaxFilters.get(i);

            RexInputRef field = (RexInputRef) call.getOperands().get(i);

            String columnName = runtimeFilterFields.get(field.getIndex()).getName();
            DataType dataType = tableMeta.getColumn(columnName).getDataType();
            SqlTypeName typeName = field.getType().getSqlTypeName();
            columnName = getFieldId(columnName);
            if (columnName == null) {
                builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                return true;
            }
            columns.add(columnName);

            // should we prune the sort key column instead of original col
            String redundantColumn = null;
            // precise data type info from table meta
            DataType preciseDataType = null;
            if (fileMeta.getColumnNameToIdx(OrcMetaUtils.redundantColumnOf(columnName)) != null) {
                redundantColumn = OrcMetaUtils.redundantColumnOf(columnName);
                columns.add(redundantColumn);

                Integer rank = ossColumnTransformer.getTargetColumnRank(filterFields.get(field.getIndex()).getName());
                preciseDataType = ossColumnTransformer.getSourceColumnMeta(rank).getDataType();
                if (!(preciseDataType instanceof SliceType)) {
                    // sort key unsupported
                    return false;
                }
            }
            switch (typeName) {
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INTEGER:
            case INTEGER_UNSIGNED:
            case BIGINT:
                Number minNumber = minMaxFilter.getMinNumber();
                Number maxNumber = minMaxFilter.getMaxNumber();
                if (minNumber != null && maxNumber != null) {
                    if (minNumber.longValue() == maxNumber.longValue()) {
                        builder.equals(columnName, PredicateLeaf.Type.LONG, minNumber.longValue());
                    } else {
                        builder.between(columnName, PredicateLeaf.Type.LONG, minNumber.longValue(),
                            maxNumber.longValue());
                    }
                } else {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                }
                return true;
            case BIGINT_UNSIGNED:
                Number minNumberUnsigned = minMaxFilter.getMinNumber();
                Number maxNumberUnsigned = minMaxFilter.getMaxNumber();
                if (minNumberUnsigned != null && maxNumberUnsigned != null) {
                    if (minNumberUnsigned.longValue() == maxNumberUnsigned.longValue()) {
                        builder.equals(columnName, PredicateLeaf.Type.LONG,
                            minNumberUnsigned.longValue() ^ UInt64Utils.FLIP_MASK);
                    } else {
                        builder.between(columnName, PredicateLeaf.Type.LONG,
                            minNumberUnsigned.longValue() ^ UInt64Utils.FLIP_MASK,
                            maxNumberUnsigned.longValue() ^ UInt64Utils.FLIP_MASK);
                    }
                } else {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                }
                return true;
            case FLOAT:
            case DOUBLE:
                Number minDouble = minMaxFilter.getMinNumber();
                Number maxDouble = minMaxFilter.getMaxNumber();
                if (minDouble != null && maxDouble != null) {
                    if (minDouble.doubleValue() == maxDouble.doubleValue()) {
                        builder.equals(columnName, PredicateLeaf.Type.FLOAT, minDouble.doubleValue());
                    } else {
                        builder.between(columnName, PredicateLeaf.Type.FLOAT, minDouble.doubleValue(),
                            maxDouble.doubleValue());
                    }
                } else {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                }
                return true;
            case VARCHAR:
            case CHAR:
                String minString = minMaxFilter.getMinString();
                String maxString = minMaxFilter.getMaxString();
                if (minString != null && maxString != null && redundantColumn != null && preciseDataType != null) {
                    // prune by sort key
                    String pruneValue1 = makeSortKeyString(minString, (SliceType) preciseDataType);
                    String pruneValue2 = makeSortKeyString(maxString, (SliceType) preciseDataType);

                    if (minString.equals(maxString)) {
                        builder.equals(redundantColumn, PredicateLeaf.Type.STRING, pruneValue1);
                    } else {
                        builder.between(redundantColumn, PredicateLeaf.Type.STRING, pruneValue1, pruneValue2);
                    }
                } else {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                }
                return true;
            case TIMESTAMP:
            case DATETIME:
            case DATE:
                Number minPacked = minMaxFilter.getMinNumber();
                Number maxPacked = minMaxFilter.getMaxNumber();
                if (minPacked != null && maxPacked != null) {
                    if (minPacked.longValue() == maxPacked.longValue()) {
                        builder.equals(columnName, PredicateLeaf.Type.LONG, minPacked.longValue());
                    } else {
                        builder.between(columnName, PredicateLeaf.Type.LONG, minPacked.longValue(),
                            maxPacked.longValue());
                    }
                } else {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                }
                return true;
            case DECIMAL:
                String minDecimalString = minMaxFilter.getMinString();
                String maxDecimalString = minMaxFilter.getMaxString();
                if (minDecimalString == null || maxDecimalString == null) {
                    builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
                    return true;
                }
                Decimal minDecimal = Decimal.fromString(minDecimalString);
                Decimal maxDecimal = Decimal.fromString(maxDecimalString);
                if (fileMeta.isEnableDecimal64() && TypeUtils.isDecimal64Precision(dataType.getPrecision())) {
                    // passing approximate values with wider range for pruning
                    long minLongVal = minDecimal.unscaleInternal(dataType.getScale());
                    long maxLongVal = maxDecimal.unscaleInternal(dataType.getScale());
                    if (minLongVal == maxLongVal) {
                        builder.equals(columnName, PredicateLeaf.Type.LONG, minLongVal);
                    } else {
                        builder.between(columnName, PredicateLeaf.Type.LONG, minLongVal,
                            maxLongVal);
                    }
                    return true;
                }

                byte[] bytes1 =
                    OssOrcFilePruner.decimalToBin(minDecimal.getDecimalStructure(),
                        dataType.getPrecision(), dataType.getScale());
                byte[] bytes2 =
                    OssOrcFilePruner.decimalToBin(maxDecimal.getDecimalStructure(),
                        dataType.getPrecision(), dataType.getScale());
                if (minDecimalString.equals(maxDecimalString)) {
                    builder.equals(columnName, PredicateLeaf.Type.STRING,
                        new String(bytes1, StandardCharsets.UTF_8));
                } else {
                    builder.between(columnName, PredicateLeaf.Type.STRING,
                        new String(bytes1, StandardCharsets.UTF_8), new String(bytes2, StandardCharsets.UTF_8));
                }
                return true;
            default:
            }
        }

        return false;
    }

    @NotNull
    private String makeSortKeyString(Object value, SliceType preciseDataType) {
        SortKey sortKey = preciseDataType.makeSortKey(value, preciseDataType.length());
        byte[] utf8Bytes = (byte[]) MySQLUnicodeUtils.latin1ToUtf8(sortKey.keys).getBase();
        return new String(utf8Bytes);
    }

    public boolean isEnableAggPruner() {
        return enableAggPruner;
    }

    public interface TriFunction<T, U, V, R> {
        R apply(T var1, U var2, V var3);
    }
}
