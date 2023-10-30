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
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableSplitPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SPLIT PARTITION", SqlKind.SPLIT_PARTITION);
    private final SqlNode splitPartitionName;
    private final SqlNode atValue;
    private final List<SqlPartition> newPartitions;
    private final SqlNode newPartitionPrefix;
    private final SqlNode newPartitionNum;
    private final boolean subPartitionsSplit;

    public SqlAlterTableSplitPartition(SqlParserPos pos, SqlNode splitPartitionName, SqlNode atValue,
                                       List<SqlPartition> newPartitions, SqlNode newPartitionPrefix,
                                       SqlNode newPartitionNum, boolean subPartitionsSplit) {
        super(pos);
        this.splitPartitionName = splitPartitionName;
        this.atValue = atValue;
        this.newPartitions = newPartitions == null ? new ArrayList<>() : newPartitions;
        this.newPartitionPrefix = newPartitionPrefix;
        this.newPartitionNum = newPartitionNum;
        this.subPartitionsSplit = subPartitionsSplit;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public SqlNode getAtValue() {
        return atValue;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public SqlNode getSplitPartitionName() {
        return splitPartitionName;
    }

    public SqlNode getNewPartitionPrefix() {
        return newPartitionPrefix;
    }

    public SqlNode getNewPartitionNum() {
        return newPartitionNum;
    }

    public boolean isSubPartitionsSplit() {
        return subPartitionsSplit;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);

        if (GeneralUtil.isNotEmpty(newPartitions)) {

            List<SqlNode> partDefs = new ArrayList<>();
            partDefs.addAll(newPartitions);
            int partColCnt = -1;
            SqlPartitionBy.validatePartitionDefs(validator, scope, partDefs, partColCnt, -1, true, false);
            if (this.atValue != null) {
                SqlNode v = this.atValue;
                if (v instanceof SqlIdentifier) {
                    String str = ((SqlIdentifier) v).getLastName();
                    if (str != null && str.toLowerCase().contains("maxvalue")) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                            "The at value is invalid"));
                    }
                }
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
            if (this.newPartitionNum != null) {
                RelDataType dataType = validator.deriveType(scope, newPartitionNum);
                if (dataType == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The partitions number is invalid"));
                }
                int splitIntoParts = ((SqlNumericLiteral) (newPartitionNum)).intValue(true);
                if (splitIntoParts < 2) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The partitions number should greater than 1"));
                }
            }

        }
    }
}
