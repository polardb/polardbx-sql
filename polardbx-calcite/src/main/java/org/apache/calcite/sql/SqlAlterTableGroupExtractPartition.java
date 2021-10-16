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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroupExtractPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("EXTRACT PARTITION", SqlKind.EXTRACT_PARTITION);
    private final SqlNode hotKey;
    private SqlAlterTableGroup parent;
    private final List<SqlPartition> newPartitions;

    private String extractPartitionName;

    public SqlAlterTableGroupExtractPartition(SqlParserPos pos, SqlNode hotkey) {
        super(pos);
        this.hotKey = hotkey;
        newPartitions = new ArrayList<>();
    }

    public String getExtractPartitionName() {
        return extractPartitionName;
    }

    public void setExtractPartitionName(String extractPartitionName) {
        this.extractPartitionName = extractPartitionName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }

    public SqlNode getHotKey() {
        return hotKey;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        // Validate partitionsCount
        if (this.hotKey != null) {
            RelDataType dataType = validator.deriveType(scope, this.hotKey);
            if (dataType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                    "The hot value is invalid"));
            }
        }
    }
}