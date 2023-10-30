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
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableSplitPartitionByHotValue extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SPLIT HOT VALUE", SqlKind.SPLIT_HOT_VALUE);
    private final List<SqlNode> hotKeys;
    private final SqlNode partitions;
    private final SQLName hotKeyPartitionName;
    private final boolean subPartitionsSplit;
    private final SQLName modifyPartitionName;

    public SQLExpr getLocality() {
        return locality;
    }

    private final SQLExpr locality;

    public SqlAlterTableSplitPartitionByHotValue(SqlParserPos pos, List<SqlNode> hotkeys, SqlNode partitions,
                                                 SQLName hotKeyPartitionName, boolean subPartitionsSplit,
                                                 SQLName modifyPartitionName, SQLExpr locality) {
        super(pos);
        this.hotKeys = hotkeys;
        this.partitions = partitions;
        this.hotKeyPartitionName = hotKeyPartitionName;
        this.locality = locality;
        this.subPartitionsSplit = subPartitionsSplit;
        this.modifyPartitionName = modifyPartitionName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public List<SqlNode> getHotKeys() {
        return hotKeys;
    }

    public SqlNode getPartitions() {
        return partitions;
    }

    public SQLName getHotKeyPartitionName() {
        return hotKeyPartitionName;
    }

    public boolean isSubPartitionsSplit() {
        return subPartitionsSplit;
    }

    public SQLName getModifyPartitionName() {
        return modifyPartitionName;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        // Validate hotkeys
        for (SqlNode hotKey : hotKeys) {
            if (hotKey != null) {
                RelDataType dataType = validator.deriveType(scope, hotKey);
                if (dataType == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The hot value is invalid"));
                }
            }
        }
        if (partitions != null) {
            RelDataType dataType = validator.deriveType(scope, partitions);
            if (dataType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                    "The partitions number is invalid"));
            }
            int splitIntoParts = ((SqlNumericLiteral) (partitions)).intValue(true);
            if (splitIntoParts < 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                    "The partitions number should greater than 0"));
            }
        }
    }
}