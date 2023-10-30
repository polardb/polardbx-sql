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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.rel.TableId;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author lingce.ldm 2017-07-06 21:39
 */
public class CalciteUtils {

    public static final String AFFECT_ROW = "AFFECT_ROW";

    private static final Logger logger = LoggerFactory.getLogger(CalciteUtils.class);

    private static final RelDataTypeFactory FACTORY = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    public static RelDataType switchRowType(List<ColumnMeta> columnMetaList, RelDataTypeFactory factory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        for (ColumnMeta cm : columnMetaList) {
            names.add(cm.getName());
            types.add(cm.getField().getRelType());
        }
        return factory.createStructType(Pair.zip(names, types));
    }

    public static void replaceTableName(SqlSelect select, List<TableId> tableIds) {
        SqlNode from = select.getFrom();

        if (tableIds.size() == 0) {
            String errMsg = "Build native sql error, TableId list is null";
            logger.error(errMsg);
            throw new OptimizerException(errMsg);
        }

        SqlKind kind = from.getKind();
        // 单表查询
        if (kind == SqlKind.AS || kind == SqlKind.IDENTIFIER) {
            select.setFrom(buildAsNode(from, tableIds));
        } else if (kind == SqlKind.JOIN) {
            // 多表JOIN
            buildJoinNode(from, tableIds);
        }

        // where 中的子查询
        SqlNode where = select.getWhere();
        if (where instanceof SqlBasicCall) {
            buildSqlBasicCall(where, tableIds);
        }
    }

    private static void buildSqlBasicCall(SqlNode where, List<TableId> tableIds) {
        for (SqlNode sqlNode : ((SqlBasicCall) where).getOperandList()) {
            if (sqlNode instanceof SqlBasicCall) {
                buildSqlBasicCall(sqlNode, tableIds);
            } else if (sqlNode instanceof SqlSelect) {
                replaceTableName((SqlSelect) sqlNode, tableIds);
            }
        }
    }

    public static void buildJoinNode(SqlNode join, List<TableId> tableIds) {
        Preconditions.checkArgument(join.getKind() == SqlKind.JOIN,
            "TableId's size great than 0, but the native sql is not a JOIN.");
        Preconditions.checkArgument(tableIds.size() > 1, "The number of TableId less than 2 for JOIN Native SQL.");

        SqlJoin sqlJoin = (SqlJoin) join;
        sqlJoin.setRight(buildAsNode(sqlJoin.getRight(), Util.last(tableIds, 1)));
        int size = tableIds.size();
        if (size == 2) {
            sqlJoin.setLeft(buildAsNode(sqlJoin.getLeft(), ImmutableList.of(tableIds.get(0))));
        } else if (size > 2) {
            List<TableId> newTableIds = Util.skipLast(tableIds);
            buildJoinNode(sqlJoin.getLeft(), newTableIds);
        } else {
            // impossible.
        }
    }

    private static SqlNode buildAsNode(SqlNode oldNode, List<TableId> tableIds) {
        SqlKind kind = oldNode.getKind();
        String aliasName = null;
        SqlNode leftNode = null;

        if (kind == SqlKind.IDENTIFIER) {
            SqlIdentifier identifier = (SqlIdentifier) oldNode;
            aliasName = Util.last(identifier.names);
            leftNode = new SqlIdentifier(tableIds.get(0).list(), SqlParserPos.ZERO);
        }

        if (kind == SqlKind.AS) {
            SqlBasicCall call = (SqlBasicCall) oldNode;
            SqlNode aliasNode = call.getOperandList().get(1);
            if (aliasNode instanceof SqlIdentifier) {
                aliasName = ((SqlIdentifier) aliasNode).getSimple();
            }

            leftNode = call.getOperandList().get(0);
            // 子查询
            if (leftNode instanceof SqlSelect) {
                replaceTableName((SqlSelect) leftNode, tableIds);
            } else {
                leftNode = new SqlIdentifier(tableIds.get(0).list(), SqlParserPos.ZERO);
            }
        }

        if (StringUtils.isEmpty(aliasName)) {
            String errMsg = "The alias name is null for Native Sql";
            logger.error(errMsg);
            throw new OptimizerException(errMsg);
        }

        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, leftNode, new SqlIdentifier(aliasName,
            SqlParserPos.ZERO));
    }

    /**
     * 压缩分库/分表名
     * <p>
     * 所有 inputs 中的字符串符合 prefix_ + 数字 的模式
     * <p>
     * 如果不符合，返回 StringUtils.join(inputs, ",")
     */
    public static String compressName(String prefix, Collection<String> inputs) {
        if (null == inputs || inputs.size() <= 0) {
            return "";
        }

        if (null == prefix) {
            return JSON.toJSONString(inputs);
        }

        List<Integer> suffixList = new LinkedList<>();
        Map<Integer, String> suffixValueStrMap = new LinkedHashMap<>();

        for (String phyTable : inputs) {
            if (phyTable.startsWith(prefix)) {
                try {
                    String suffix = phyTable.substring(prefix.length(), phyTable.length());
                    Integer suffixValue = Integer.valueOf(suffix);
                    suffixList.add(suffixValue);
                    suffixValueStrMap.put(suffixValue, suffix);
                } catch (Exception e) {
                    return org.apache.commons.lang3.StringUtils.join(inputs, ",");
                }
            } else {
                return org.apache.commons.lang3.StringUtils.join(inputs, ",");
            }
        } // end of for

        Collections.sort(suffixList);

        StringBuilder result = new StringBuilder(prefix);

        if (suffixList.size() > 1) {
            result.append("[");
        }

        int lastSuffixValue = -1;
        int consecutiveCount = 0;
        for (int index = 0; index < suffixList.size(); index++) {
            if (lastSuffixValue >= 0) {
                int suffix = suffixList.get(index);
                if (suffix - lastSuffixValue == 1) {
                    consecutiveCount++;
                    lastSuffixValue = suffix;
                } else {
                    // 发现新节点不连续了
                    if (consecutiveCount == 1) {
                        result.append(",").append(suffixValueStrMap.get(lastSuffixValue));
                    } else if (consecutiveCount > 1) {
                        result.append("-").append(suffixValueStrMap.get(lastSuffixValue));
                    }

                    consecutiveCount = 0;

                    // 上一个压缩段完结, 开个新的
                    result.append(",");
                    result.append(suffixValueStrMap.get(suffix));
                    lastSuffixValue = suffix;
                }
            } else {
                // 第一次
                result.append(suffixValueStrMap.get(suffixList.get(index)));
                lastSuffixValue = suffixList.get(index);
            }
        }

        // 有剩余节点
        if (consecutiveCount == 1) {
            result.append(",").append(suffixValueStrMap.get(lastSuffixValue));
        } else if (consecutiveCount > 1) {
            result.append("-").append(suffixValueStrMap.get(lastSuffixValue));
        }

        if (suffixList.size() > 1) {
            result.append("]");
        }

        return result.toString();
    }

    public static List<ColumnMeta> buildColumnMeta(RelNode logicalPlan, String tableName) {
        return buildColumnMeta(logicalPlan.getRowType(), tableName);
    }

    public static List<ColumnMeta> buildColumnMeta(RelDataType dataType, String tableName) {
        List<ColumnMeta> columnMeta = new LinkedList<>();
        List<RelDataTypeField> relDataTypeList = dataType.getFieldList();
        for (RelDataTypeField relDataTypeField : relDataTypeList) {
            RelDataType relDataType = relDataTypeField.getType();

            Field field = new Field(tableName, relDataTypeField.getName(), relDataType);
            columnMeta.add(new ColumnMeta(tableName, relDataTypeField.getName(), null, field));
        }
        return columnMeta;
    }

    public static List<ColumnMeta> buildColumnMeta(RelDataType dataType, List<String> tableNames,
                                                   List<TableMeta> tableMetas) {
        if (tableNames == null) {
            return buildColumnMeta(dataType, "");
        }

        List<ColumnMeta> columnMeta = new LinkedList<>();
        List<RelDataTypeField> relDataTypeList = dataType.getFieldList();
        if (relDataTypeList.size() != tableNames.size()) {
            return buildColumnMeta(dataType, "");
        }
        if (tableMetas != null && tableMetas.size() != tableNames.size()) {
            return buildColumnMeta(dataType, "");
        }

        for (int i = 0; i < relDataTypeList.size(); i++) {
            final RelDataTypeField relDataTypeField = relDataTypeList.get(i);
            final RelDataType relDataType = relDataTypeField.getType();
            final boolean isPrimary = tableMetas != null && tableMetas.get(i) != null
                && tableMetas.get(i).getPrimaryKeyMap().containsKey(relDataTypeField.getName());
            Field field = new Field(tableNames.get(i), relDataTypeField.getName(), relDataType, isPrimary);
            columnMeta.add(new ColumnMeta(tableNames.get(i), relDataTypeField.getName(), null, field));
        }
        return columnMeta;
    }

    @Deprecated
    public static DataType relTypeToTddlType(SqlTypeName typeName, int precision, boolean hasBooleanType,
                                             boolean sensitive, List<String> enumValues, String charset,
                                             String collation) {
        DataType dataType = null;
        CharsetName charsetName = Optional.ofNullable(charset)
            .map(CharsetName::of)
            .orElseGet(CharsetName::defaultCharset);
        CollationName collationName = Optional.ofNullable(collation)
            .map(CollationName::of)
            .orElseGet(CollationName::defaultCollation);
        switch (typeName) {
        case DECIMAL:
            dataType = DataTypes.DecimalType;
            break;
        case BOOLEAN:
            if (hasBooleanType) {
                dataType = DataTypes.BooleanType;
            } else {
                dataType = DataTypes.TinyIntType;
            }
            break;
        case TINYINT:
            dataType = DataTypes.TinyIntType;
            break;
        case SMALLINT:
            dataType = DataTypes.SmallIntType;
            break;
        case INTEGER:
            dataType = DataTypes.IntegerType;
            break;
        case MEDIUMINT:
            dataType = DataTypes.MediumIntType;
            break;
        case BIGINT:
            dataType = DataTypes.LongType;
            break;
        case FLOAT:
            dataType = DataTypes.FloatType;
            break;
        case REAL:
        case DATETIME:
            dataType = DataTypes.DatetimeType;
            break;
        case DOUBLE:
            dataType = DataTypes.DoubleType;
            break;
        case DATE:
            dataType = DataTypes.DateType;
            break;
        case TIME:
            dataType = DataTypes.TimeType;
            break;
        case TIMESTAMP:
            dataType = DataTypes.TimestampType;
            break;
        case YEAR:
            dataType = DataTypes.YearType;
            break;
        case INTERVAL_YEAR:
            dataType = DataTypes.IntervalType;
            break;
        case INTERVAL_YEAR_MONTH:
            dataType = DataTypes.YearType;
            break;
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_DAY_MICROSECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
            dataType = DataTypes.IntervalType;
            break;
        case SIGNED:
            dataType = DataTypes.LongType;
            break;
        case UNSIGNED:
            dataType = DataTypes.ULongType;
            break;
        case CHAR:
            dataType = new CharType(charsetName, collationName);
            break;
        case VARCHAR:
            dataType = new VarcharType(charsetName, collationName);
            break;
        case BIT:
            dataType = DataTypes.BitType;
            break;
        case BIG_BIT:
            dataType = DataTypes.BigBitType;
            break;
        case BINARY_VARCHAR:
            dataType = DataTypes.BinaryStringType;
            break;
        case BLOB:
            dataType = DataTypes.BlobType;
            break;
        case VARBINARY:
        case BINARY:
            dataType = DataTypes.BinaryType;
            break;
        case NULL:
            dataType = DataTypes.NullType;
            break;
        case INTEGER_UNSIGNED:
            dataType = DataTypes.UIntegerType;
            break;
        case TINYINT_UNSIGNED:
            dataType = DataTypes.UTinyIntType;
            break;
        case BIGINT_UNSIGNED:
            dataType = DataTypes.ULongType;
            break;
        case SMALLINT_UNSIGNED:
            dataType = DataTypes.USmallIntType;
            break;
        case MEDIUMINT_UNSIGNED:
            dataType = DataTypes.UMediumIntType;
        case ANY:
            break;
        case SYMBOL:
            break;
        case MULTISET:
            break;
        case ARRAY:
            break;
        case MAP:
            break;
        case DISTINCT:
            break;
        case STRUCTURED:
            break;
        case ROW:
            break;
        case OTHER:
            break;
        case CURSOR:
            break;
        case COLUMN_LIST:
            break;
        case DYNAMIC_STAR:
            break;
        case JSON:
            dataType = DataTypes.JsonType;
            break;
        case INTERVAL:
            dataType = DataTypes.IntervalType;
            break;
        case ENUM:
            dataType = new EnumType(enumValues);
            break;
        default:
            dataType = null;
        }
        return dataType;
    }

    public static String removeFirstAndLastChar(String orgStr, char c) {
        if (orgStr == null || orgStr.length() <= 0) {
            return orgStr;
        }
        if (orgStr.charAt(0) != c) {
            return orgStr;
        }
        if (orgStr.charAt(orgStr.length() - 1) != c) {
            throw new IllegalArgumentException(
                "String start with a " + c + " must end with a " + c + ", id: " + orgStr);
        }
        return orgStr.substring(1, orgStr.length() - 1);
    }

    public static List<RexNode> shiftRightFilter(List<RexNode> rightFilters, int offset, RelDataType srcRowType,
                                                 RelDataType dstRowType, RexBuilder rexBuilder) {
        int nSrcFields = srcRowType.getFieldList().size();
        int[] adjustments = new int[nSrcFields];
        for (int i = 0; i < nSrcFields; i++) {
            adjustments[i] = offset;
        }

        List<RexNode> result = new ArrayList<>();
        for (RexNode filter : rightFilters) {
            result.add(filter.accept(new RelOptUtil.RexInputConverter(rexBuilder,
                srcRowType.getFieldList(),
                dstRowType.getFieldList(),
                adjustments)));
        }
        return result;
    }

    public static List<DataType> getTypes(RelDataType rowType) {
        List<DataType> dataTypes = new ArrayList<>();
        List<RelDataTypeField> relDataTypeList = rowType.getFieldList();
        for (RelDataTypeField relDataTypeField : relDataTypeList) {
            RelDataType relDataType = relDataTypeField.getType();
            DataType dt = DataTypeUtil.calciteToDrdsType(relDataType);
            dataTypes.add(dt);
        }
        return dataTypes;
    }

    public static DataType getType(RelDataTypeField relDataTypeField) {
        RelDataType relDataType = relDataTypeField.getType();
        return DataTypeUtil.calciteToDrdsType(relDataType);
    }

    public static boolean isJoinKeyHaveSameType(Join join) {
        JoinInfo joinInfo = join.analyzeCondition();
        RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
            join.getCluster().getTypeFactory(), join, joinInfo.leftKeys, joinInfo.rightKeys);
        List<RelDataTypeField> keyDataFieldList = keyDataType.getFieldList();

        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        for (int i = 0; i < keyDataFieldList.size(); i++) {
            RelDataType t1 = keyDataFieldList.get(i).getType();
            RelDataTypeField t2Field = left.getRowType().getFieldList().get(joinInfo.leftKeys.get(i));
            RelDataType t2 = t2Field.getType();
            // only compare sql type name
            if (!t1.getSqlTypeName().equals(t2.getSqlTypeName())) {
                return false;
            }
        }

        for (int i = 0; i < keyDataFieldList.size(); i++) {
            RelDataType t1 = keyDataFieldList.get(i).getType();
            RelDataTypeField t2Field = right.getRowType().getFieldList().get(joinInfo.rightKeys.get(i));
            RelDataType t2 = t2Field.getType();
            // only compare sql type name
            if (!t1.getSqlTypeName().equals(t2.getSqlTypeName())) {
                return false;
            }
        }
        return true;
    }

    //当join两端类型不一致的时候，找出common type以便在exchange的时候做类型转换。
    public static RelDataType getJoinKeyDataType(RelDataTypeFactory factory, Join join, Collection<Integer> lKeys,
                                                 Collection<Integer> rKeys) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        List<RelDataType> leftTypes = new ArrayList<>();
        List<RelDataType> rightTypes = new ArrayList<>();
        for (int number : lKeys) {
            leftTypes.add(left.getRowType().getFieldList().get(number).getType());
        }
        for (int number : rKeys) {
            rightTypes.add(right.getRowType().getFieldList().get(number).getType());
        }
        List<RelDataType> commonTypes = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < leftTypes.size(); i++) {
            commonTypes.add(getUnifiedRelDataType(leftTypes.get(i), rightTypes.get(i)));
            names.add("$" + i);
        }
        return factory.createStructType(Pair.zip(names, commonTypes));
    }

    public static RelDataType getUnifiedRelDataType(RelDataType leftColDataType, RelDataType rightColDataType) {
        if (SqlTypeUtil.isCharacter(leftColDataType) && SqlTypeUtil.isCharacter(rightColDataType)) {
            // mix of collation ?
            return Arrays.stream(new RelDataType[] {leftColDataType, rightColDataType})
                .map(RelDataType::getCollation)
                .map(SqlCollation::getCollationName)
                .map(CollationName::of)
                .reduce(
                    (c1, c2) -> {
                        CollationName res = CollationName.getMixOfCollation0(c1, c2);
                        return res == null ? CollationName.defaultCollation() : res;
                    }
                ).map(
                    c -> {
                        CharsetName charsetName = CollationName.getCharsetOf(c);
                        RelDataType t = FACTORY.createSqlType(SqlTypeName.VARCHAR);
                        t = FACTORY.createTypeWithCharsetAndCollation(
                            t,
                            charsetName.toJavaCharset(),
                            new SqlCollation(
                                charsetName.toJavaCharset(),
                                c.name(),
                                SqlCollation.Coercibility.COERCIBLE
                            )
                        );
                        return t;
                    }
                )
                .orElseGet(
                    () -> FACTORY.createSqlType(SqlTypeName.VARCHAR)
                );

        } else {
            return getUnifiedRelDataTypeInternal(leftColDataType, rightColDataType);
        }
    }

    private static RelDataType getUnifiedRelDataTypeInternal(RelDataType leftColDataType,
                                                             RelDataType rightColDataType) {
        DataType leftDt = DataTypeUtil.calciteToDrdsType(leftColDataType);
        DataType rightDt = DataTypeUtil.calciteToDrdsType(rightColDataType);

        TypeUtils.MathLevel leftMl = TypeUtils.getMathLevel(leftDt);
        TypeUtils.MathLevel rightMl = TypeUtils.getMathLevel(rightDt);

        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        if (DataTypeUtil.isUnderIntType(leftDt) && DataTypeUtil.isUnderIntType(rightDt)) {
            return factory.createSqlType(SqlTypeName.INTEGER);
        } else if (leftMl.isOther() && rightMl.isOther()) {
            return factory.createSqlType(SqlTypeName.VARCHAR);
        } else if (leftMl.isOther() || rightMl.isOther()) {
            return factory.createSqlType(SqlTypeName.DECIMAL);
        } else {
            return leftMl.level < rightMl.level ? leftColDataType : rightColDataType;
        }
    }

    public static DataType getUnifiedDataType(RelDataType leftColDataType, RelDataType rightColDataType) {
        if (SqlTypeUtil.isCharacter(leftColDataType) && SqlTypeUtil.isCharacter(rightColDataType)) {
            // mix of collation ?
            return Arrays.stream(new RelDataType[] {leftColDataType, rightColDataType})
                .map(RelDataType::getCollation)
                .map(SqlCollation::getCollationName)
                .map(CollationName::of)
                .reduce(
                    (c1, c2) -> {
                        CollationName res = CollationName.getMixOfCollation0(c1, c2);
                        return res == null ? CollationName.defaultCollation() : res;
                    }
                ).map(
                    c -> {
                        CharsetName charsetName = CollationName.getCharsetOf(c);
                        return new VarcharType(charsetName, c);
                    }
                ).map(DataType.class::cast)
                .orElseGet(
                    () -> new VarcharType()
                );

        } else {
            return getUnifiedDataTypeInternal(leftColDataType, rightColDataType);
        }
    }

    // todo need refactor according to item_cmpfunc.cc/Arg_comparator::set_cmp_func
    private static DataType getUnifiedDataTypeInternal(RelDataType leftColDataType, RelDataType rightColDataType) {
        DataType leftDt = DataTypeUtil.calciteToDrdsType(leftColDataType);
        DataType rightDt = DataTypeUtil.calciteToDrdsType(rightColDataType);

        int scale;
        // For the same type from both join side, we may choose different type from that.
        if (DataTypeUtil.equalsSemantically(leftDt, rightDt)) {
            if (DataTypeUtil.equalsSemantically(leftDt, DataTypes.TimeType)) {
                scale = Math.max(leftDt.getScale(), rightDt.getScale());
                return new TimeType(scale);
            } else if (DataTypeUtil.equalsSemantically(leftDt, DataTypes.DatetimeType)
                || DataTypeUtil.equalsSemantically(leftDt, DataTypes.TimestampType)) {
                scale = Math.max(leftDt.getScale(), rightDt.getScale());
                return new TimestampType(scale);
            } else if (DataTypeUtil.equalsSemantically(leftDt, DataTypes.DateType)) {
                return new DateType();
            } else if (DataTypeUtil.equalsSemantically(leftDt, DataTypes.BlobType)) {
                return DataTypes.BlobType;
            }
        } else if (DataTypeUtil.anyMatchSemantically(leftDt, DataTypes.DatetimeType, DataTypes.DateType)
            && DataTypeUtil.anyMatchSemantically(rightDt, DataTypes.DatetimeType, DataTypes.DateType)) {
            // for comparison of datetime - date type, use datetime type
            scale = Math.max(leftDt.getScale(), rightDt.getScale());
            return new DateTimeType(scale);
        } else if (DataTypeUtil.equalsSemantically(leftDt, DataTypes.TimestampType)
            && DataTypeUtil.equalsSemantically(rightDt, DataTypes.DatetimeType)) {
            return new DateTimeType(Math.max(leftDt.getScale(), rightDt.getScale()));
        } else if (DataTypeUtil.equalsSemantically(leftDt, DataTypes.DatetimeType)
            && DataTypeUtil.equalsSemantically(rightDt, DataTypes.TimestampType)) {
            return new DateTimeType(Math.max(leftDt.getScale(), rightDt.getScale()));
        }

        TypeUtils.MathLevel leftMl = TypeUtils.getMathLevel(leftDt);
        TypeUtils.MathLevel rightMl = TypeUtils.getMathLevel(rightDt);

        if (DataTypeUtil.isUnderIntType(leftDt) && DataTypeUtil.isUnderIntType(rightDt)) {
            return DataTypes.IntegerType;
        } else if (leftMl.isOther() && rightMl.isOther()) {
            return DataTypes.StringType;
        } else if (leftMl.isOther() || rightMl.isOther()) {
            return DataTypes.DecimalType;
        } else {
            return leftMl.level < rightMl.level ? leftMl.type : rightMl.type;
        }
    }

    public static CursorMeta buildDmlCursorMeta() {
        TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RelDataType dataType = factory.createSqlType(SqlTypeName.INTEGER);
        Field field = new Field(null, AFFECT_ROW, dataType, false, false);
        ColumnMeta colMeta = new ColumnMeta(null, AFFECT_ROW, null, field);
        return CursorMeta.build(ImmutableList.of(colMeta));
    }

    public static Pair<String, String> getDbNameAndTableNameByTableIdentifier(SqlIdentifier tblId) {
        String schemaName;
        String tbName;
        int nameSize = tblId.names.size();
        if (nameSize == 2) {
            schemaName = tblId.names.get(0);
            tbName = tblId.names.get(1);
        } else {
            schemaName = OptimizerContext.getContext(null).getSchemaName();
            tbName = tblId.names.get(0);
        }

        Pair<String, String> dbAndTb = new Pair<>(schemaName, tbName);
        return dbAndTb;
    }
}
