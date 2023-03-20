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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlPartitionBy extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlPartitionByOperator();
    protected SqlSubPartitionBy subPartitionBy;
    protected SqlNode partitionsCount;
    protected List<SqlNode> partitions = new ArrayList<>();
    protected List<SqlNode> columns = new ArrayList<>();
    private String sourceSql;

    public SqlPartitionBy(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return partitions;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);

        // Validate all the part cols of PartitionBy
        List<RelDataType> partColTypes = new ArrayList<>();
        boolean isColumnsPartition = false;
        boolean isHash = false;
        Object partByObj = this;
        if (partByObj instanceof MySqlPartitionByKey) {
            isColumnsPartition = true;
        } else if (partByObj instanceof SqlPartitionByHash) {
            isHash = true;
            isColumnsPartition = ((SqlPartitionByHash) partByObj).isKey();
        } else if (partByObj instanceof SqlPartitionByRange) {
            isColumnsPartition = ((SqlPartitionByRange) partByObj).isColumns();
        } else if (partByObj instanceof SqlPartitionByList) {
            isColumnsPartition = ((SqlPartitionByList) partByObj).isColumns();
        }

        for (SqlNode partCol : this.getColumns()) {
            SqlCreateTable.PartitionColumnFinder columnFinder = new SqlCreateTable.PartitionColumnFinder();
            partCol.accept(columnFinder);
            if (columnFinder.getPartColumn() == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                    .format("Not allowed to use unknown column[%s] as partition column", partCol.toString()));
            } else {
                if (isColumnsPartition) {
                    if (columnFinder.isContainPartFunc()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                            .format(
                                "Not allowed to use partition column[%s] with partition function in  key or range/list columns policy",
                                partCol.toString()));
                    }
                } else {
                    if (columnFinder.isUseNestingPartFunc()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                            .format("Not allowed to use nesting partition function [%s] in hash/range/list policy",
                                partCol.toString()));
                    }
                }
            }

            RelDataType dataType = validator.deriveType(scope, partCol);
            if (dataType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    String.format("Failed to derive type for column[%s]", partCol.toString()));
            }
            SqlTypeName typeName = dataType.getSqlTypeName();
            if (isColumnsPartition) {
                if (!(SqlTypeName.INT_TYPES.contains(typeName) || SqlTypeName.DATETIME_YEAR_TYPES.contains(typeName)
                    || SqlTypeName.CHAR_TYPES.contains(typeName) || typeName == SqlTypeName.VARBINARY ||typeName == SqlTypeName.BINARY  )) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                        .format("The datatype[%s] of column[%s] is not supported", typeName.getName(),
                            partCol.toString()));
                }
            } else {
                if (!SqlTypeName.INT_TYPES.contains(typeName) && !isHash) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                        .format("The datatype[%s] of column[%s] is not supported", typeName.getName(),
                            partCol.toString()));
                }
            }
            partColTypes.add(dataType);
        }
        int partColCnt = partColTypes.size();

        // Validate partitions
        SqlPartitionBy.validatePartitionDefs(validator, scope, this.getPartitions(), partColCnt, false);

        // Validate partitionsCount
        SqlNode partCnt = this.partitionsCount;
        SqlPartitionBy.validatePartitionCount(validator, scope, partCnt);

        // Validate subPartitionBy
        // To be impl
    }

    public static void validatePartitionCount(SqlValidator validator,
                                              SqlValidatorScope scope,
                                              SqlNode partCnt) {
        if (partCnt != null) {
            RelDataType dataType = validator.deriveType(scope, partCnt);
            if (dataType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                    "The partition count value is invalid"));
            } else {
                SqlTypeName typeName = dataType.getSqlTypeName();
                if (!SqlTypeName.INT_TYPES.contains(typeName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The partition count value must be an integer"));
                }
            }
        }
    }

    public static void validatePartitionDefs(SqlValidator validator,
                                             SqlValidatorScope scope,
                                             List<SqlNode> partDefs,
                                             int partColCnt, boolean allowNoPartBndVal) {
        Set<String> partNameSet = new HashSet<>();
        for (int i = 0; i < partDefs.size(); i++) {
            SqlPartition partDef = (SqlPartition) partDefs.get(i);

            // Validate all part names of PartitionBy
            SqlNode partName = partDef.getName();
            String partNameStr = null;
            if (!(partName instanceof SqlIdentifier)) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    String.format("The partition name is invalid", partName.toString()));
            } else {
                partNameStr = ((SqlIdentifier) partName).getLastName();
                if (partNameSet.contains(partNameStr)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                        String.format("The partition name [%s] is duplicated", partNameStr));
                }
                partNameSet.add(partNameStr.toLowerCase());
            }

            SqlPartitionValue bndVal = partDef.getValues();
            if (bndVal == null) {
                if (!allowNoPartBndVal) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                        String.format("found invalid partition values of partition[%s] ", partNameStr));
                }
                return;
            }
            List<SqlPartitionValueItem> items = bndVal.getItems();
            for (int j = 0; j < items.size(); j++) {
                SqlPartitionValueItem partitionValueItem = items.get(j);
                if (partitionValueItem.isMaxValue()) {
                    continue;
                }
                boolean containMaxValue = false;
                SqlNode valItem = partitionValueItem.getValue();
                if (valItem.getKind() == SqlKind.ROW) {
                    List<SqlNode> opList = ((SqlCall) valItem).getOperandList();
                    for (int k = 0; k < opList.size(); k++) {
                        SqlNode v = opList.get(k);
                        if (v instanceof SqlIdentifier) {
                            String str = ((SqlIdentifier) v).getLastName();
                            if (str != null && str.toLowerCase().contains("maxvalue")) {
                                containMaxValue = true;
                                break;
                            }
                        }
                    }
                } else if (valItem instanceof SqlIdentifier) {
                    String str = ((SqlIdentifier) valItem).getLastName();
                    if (str != null && str.toLowerCase().contains("maxvalue")) {
                        containMaxValue = true;
                    }
                }
                if (containMaxValue && bndVal.getOperator() == SqlPartitionValue.Operator.In) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                        String.format("cannot use 'maxvalue' as value in VALUES IN"));
                }

                RelDataType valItemDt = validator.deriveType(scope, valItem);
                if (valItemDt.isStruct()) {
                    // valItem is row expr
                    List<RelDataTypeField> valItemTypeFlds = valItemDt.getFieldList();
                    if (partColCnt > 0 && valItemTypeFlds.size() != partColCnt) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                            "the bound value of partition[%s] must match the partition columns", partNameStr));
                    }
                } else {
                    // valItem is single col or func(col) expr
                    RelDataType dataType = validator.deriveType(scope, valItem);
                    if (dataType.getSqlTypeName() != SqlTypeName.NULL) {
                        Preconditions.checkNotNull(dataType);
                    }
                }
            }
            bndVal.validate(validator, scope);
        }
    }

    public List<SqlNode> getColumns() {
        return columns;
    }

    public List<SqlNode> getPartitions() {
        return partitions;
    }

    public SqlSubPartitionBy getSubPartitionBy() {
        return subPartitionBy;
    }

    public void setSubPartitionBy(SqlSubPartitionBy subPartitionBy) {
        this.subPartitionBy = subPartitionBy;
    }

    public static SqlOperator getOPERATOR() {
        return OPERATOR;
    }

    public SqlNode getPartitionsCount() {
        return partitionsCount;
    }

    public void setPartitionsCount(SqlNode partitionsCount) {
        this.partitionsCount = partitionsCount;
    }

    public SqlNode getSqlTemplate() {
        return this;
    }

    public static class SqlPartitionByOperator extends SqlSpecialOperator {

        public SqlPartitionByOperator() {
            super("PARTITION_BY", SqlKind.PARTITION_BY);
        }
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL);
        writer.sep("PARTITION BY");
        writer.sep(sourceSql);
        writer.endList(frame);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (this == node) {
            return true;
        }

        if (node == null) {
            return false;
        }

        if (node.getClass() != this.getClass()) {
            return false;
        }

        SqlPartitionBy objPartBy = (SqlPartitionBy) node;

        if (!equalDeep(this.subPartitionBy, objPartBy.subPartitionBy, litmus)) {
            return false;
        }

        if (!equalDeep(this.partitionsCount, objPartBy.partitionsCount, litmus)) {
            return false;
        }

        if (!equalDeep(this.partitions, objPartBy.partitions, litmus)) {
            return false;
        }

        return equalDeep(this.columns, objPartBy.columns, litmus);
    }
}
