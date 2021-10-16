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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

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
        Object partByObj = this;
        if (partByObj instanceof MySqlPartitionByKey) {
            isColumnsPartition = true;
        } else if (partByObj instanceof SqlPartitionByHash) {
            isColumnsPartition = ((SqlPartitionByHash)partByObj).isKey();
        } else if(partByObj instanceof SqlPartitionByRange){
            isColumnsPartition = ((SqlPartitionByRange)partByObj).isColumns();
        } else if (partByObj instanceof SqlPartitionByList) {
            isColumnsPartition = ((SqlPartitionByList)partByObj).isColumns();
        }
        for (SqlNode partCol : this.getColumns()) {
            RelDataType dataType = validator.deriveType(scope, partCol);
            if (dataType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    String.format("Failed to derive type for column[%s]", partCol.toString()));
            }
            SqlTypeName typeName = dataType.getSqlTypeName();
            if (isColumnsPartition) {
                if (!(SqlTypeName.INT_TYPES.contains(typeName) || SqlTypeName.DATETIME_YEAR_TYPES.contains(typeName)
                    || SqlTypeName.CHAR_TYPES.contains(typeName))) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                        .format("The datatype[%s] of column[%s] is not supported", typeName.getName(),
                            partCol.toString()));
                }
            } else {
                if (!SqlTypeName.INT_TYPES.contains(typeName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String
                        .format("The datatype[%s] of column[%s] is not supported", typeName.getName(),
                            partCol.toString()));
                }
            }
            partColTypes.add(dataType);
        }

        List<SqlNode> partDefs = this.getPartitions();
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
            List<SqlPartitionValueItem> items = bndVal.getItems();
            for (int j = 0; j < items.size(); j++) {
                SqlPartitionValueItem partitionValueItem = items.get(j);
                if (partitionValueItem.isMaxValue()) {
                    continue;
                }
                SqlNode valItem = partitionValueItem.getValue();
                RelDataType valItemDt = validator.deriveType(scope, valItem);
                if (valItemDt.isStruct()) {
                    // valItem is row expr
                    List<RelDataTypeField> valItemTypeFlds = valItemDt.getFieldList();
                    assert valItemTypeFlds.size() == partColTypes.size();
                    for (int k = 0; k < valItemTypeFlds.size(); k++) {
                        RelDataTypeField fld = valItemTypeFlds.get(k);
                        RelDataType valItemRelDt = fld.getType();
                        if (!valItemRelDt.getFamily().equals(partColTypes.get(k).getFamily())) {
//                            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
//                                "The datatype[%s] of boundValue [%s] of partition[%s] is conflict to its partition column",
//                                valItemRelDt.getSqlTypeName().getName(), valItem.toString(), partNameStr));
                        }
                    }
                } else {
                    // valItem is single col or func(col) expr
                    RelDataType dataType = validator.deriveType(scope, valItem);
                    if (dataType.getSqlTypeName() != SqlTypeName.NULL) {
                        Preconditions.checkNotNull(dataType);
                        if (!dataType.getFamily().equals(partColTypes.get(0).getFamily())) {
//                            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
//                                "The datatype[%s] of boundValue [%s] of partition[%s] is conflict to its partition column",
//                                dataType.getSqlTypeName().getName(), valItem.toString(), partNameStr));
                        }
                    }

                }
            }
            bndVal.validate(validator, scope);
        }

        // Validate partitionsCount
        if (this.partitionsCount != null) {
            RelDataType dataType = validator.deriveType(scope, this.partitionsCount);
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


        // Validate subPartitionBy
        // To be impl
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
}
