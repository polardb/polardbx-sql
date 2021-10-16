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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroupSplitPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SPLIT PARTITION", SqlKind.SPLIT_PARTITION);
    private final SqlNode splitPartitionName;
    private final SqlNode atValue;
    private final List<SqlPartition> newPartitions;
    private SqlAlterTableGroup parent;

    public SqlAlterTableGroupSplitPartition(SqlParserPos pos, SqlNode splitPartitionName, SqlNode atValue,
                                            List<SqlPartition> newPartitions) {
        super(pos);
        this.splitPartitionName = splitPartitionName;
        this.atValue = atValue;
        this.newPartitions = newPartitions == null ? new ArrayList<>() : newPartitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getAtValue() {
        return atValue;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public SqlNode getSplitPartitionName() {
        return splitPartitionName;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);

        if (GeneralUtil.isNotEmpty(newPartitions)) {
            Set<String> partNameSet = new HashSet<>();
            for (int i = 0; i < newPartitions.size(); i++) {
                SqlPartition partDef = newPartitions.get(i);

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
                if (bndVal != null) {
                    List<SqlNode> items =
                        bndVal.getItems().stream().map(o -> o.getValue()).collect(Collectors.toList());
                    for (int j = 0; j < items.size(); j++) {
                        SqlNode valItem = items.get(j);
                        validator.deriveType(scope, valItem);
                    }
                    bndVal.validate(validator, scope);
                }

                // Validate partitionsCount
                if (this.atValue != null) {
                    RelDataType dataType = validator.deriveType(scope, this.atValue);
                    if (dataType == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                            "The at value is invalid"));
                    } else {
                        SqlTypeName typeName = dataType.getSqlTypeName();
                        if (!SqlTypeName.INT_TYPES.contains(typeName)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                                "The at value must be an integer"));
                        }
                    }
                }

                // Validate subPartitionBy
                // To be impl
            }
        }
    }
}
